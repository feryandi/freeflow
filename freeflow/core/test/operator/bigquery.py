#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest

import freeflow.core.test

import pendulum

from airflow import models as af_models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class OperatorBigqueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = freeflow.core.test.dag_files

    def test_operator_bigquery(self):

        def get_rendered_template(task):
            """ Render the BigQuery SQL script template """
            # In order to handle file not found
            task.bql = '/dags/' + task.bql
            dttm = pendulum.parse('2018-10-21T00:00:00')
            ti = af_models.TaskInstance(task=task, execution_date=dttm)
            try:
                ti.render_templates()
            except Exception as e:
                raise Exception("Error rendering template: " + str(e))
            return task.__class__.template_fields

        def dry_run_bql(task):
            """ Call the BigQuery dry run API to run the rendered query """
            query = getattr(task, 'bql')

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
