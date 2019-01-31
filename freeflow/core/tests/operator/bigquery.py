#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest

import freeflow.core.tests

import datetime

from airflow import models as af_models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class OperatorBigqueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = freeflow.core.tests.dag_files

    def test_operator_bigquery(self):

        def get_rendered_template(task):
            """
            Returns a rendered BigQuery SQL script.

            :param task: BigQueryOperator task that need to be rendered
            :type task: BigQueryOperator
            :return: list of templated fields from BigQueryOperator
            :rtype: list(str)
            """
            # Added dags to the bql script path to create correct path
            if hasattr(task, 'sql'):
                task.sql = '/dags/' + task.sql
            if hasattr(task, 'bql'):
                task.bql = '/dags/' + task.bql

            dttm = datetime.datetime(2018, 10, 21, 0, 0, 0)
            ti = af_models.TaskInstance(task=task, execution_date=dttm)
            try:
                ti.render_templates()
            except Exception as e:
                raise Exception("Error rendering template: " + str(e))
            return task.__class__.template_fields

        def dry_run_bql(task):
            """
            Call the BigQuery dry run API to run the rendered query.

            :param task: BigQueryOperator task that need to be rendered
            :type task: BigQueryOperator
            :return: query reply from the API
            :rtype: json
            """
            query = getattr(task, 'bql')
            if query is None:
                query = getattr(task, 'sql')

            hook = BigQueryHook(bigquery_conn_id=task.bigquery_conn_id,
                                delegate_to=task.delegate_to)
            conn = hook.get_conn()
            cursor = conn.cursor()

            job_data = {
                'configuration': {
                    'dryRun': True,
                    'query': {
                        'query': query,
                        'useLegacySql': task.use_legacy_sql,
                        'maximumBillingTier': task.maximum_billing_tier
                    }
                }
            }

            jobs = cursor.service.jobs()
            query_reply = jobs \
                .insert(projectId=cursor.project_id, body=job_data) \
                .execute()

            return query_reply

        def get_bq_tasks(dag_files):
            """
            Filters tasks list to only returns task which is a
            BigQueryOperator class.

            :param dag_files: list of DAG instance, its filename and
                              DAG module
            :type dag_class: list(dict)
            :return: filtered tasks
            :rtype: list(BigQueryOperator)
            """
            tasks = []
            for file in dag_files:
                for task in file['instance']['tasks']:
                    if isinstance(task, BigQueryOperator):
                        tasks += [task]
            return tasks

        tasks = get_bq_tasks(self._dag_files)

        if len(tasks) != 0:

            for task in tasks:
                try:
                    get_rendered_template(task)
                    dry_run_bql(task)

                except Exception as e:
                    raise Exception('Task: ' + task.task_id + ', ' + str(e))
                    self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
