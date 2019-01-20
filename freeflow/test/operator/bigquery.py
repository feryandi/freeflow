#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import os

import freeflow.core.dag_loader as dag_loader

import google
import pendulum

from airflow import models as af_models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from google.cloud import bigquery

# CRITICAL
# CRITICAL
# CRITICAL
# TO-DO: rethink how to test this without preparing all the credentials. (e.g. use the Bigquery Hook)

SCOPE = ('https://www.googleapis.com/auth/bigquery',
         'https://www.googleapis.com/auth/cloud-platform',
         'https://www.googleapis.com/auth/drive',
         'https://www.googleapis.com/auth/devstorage.read_write')

class OperatorBigqueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = dag_loader.get_dag_files()

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


        def dry_run_bql(query):
            """ Call the BigQuery dry run API to run the rendered query """
            job_config = bigquery.QueryJobConfig()
            job_config.dry_run = True
            job_config.use_query_cache = False

            query_job = client.query((query),
                                     location='US',
                                     job_config=job_config)

            if query_job.state != 'DONE':
                raise Exception('Dry run state is not `DONE`')
            return query_job.dry_run

            # Do we need also monitor the bytes processed?
            # query_job.total_bytes_processed

        credentials, _ = google.auth.default(scopes=SCOPE)

        client = bigquery.Client(credentials=credentials)

        for file in self._dag_files:
            for task in file['instance']['tasks']:
                try:
                    if isinstance(task, BigQueryOperator):
                        template_fields = get_rendered_template(task)

                        for template_field in template_fields:
                            if template_field == 'bql':
                                query = getattr(task, template_field)
                                assert dry_run_bql(query)

                except Exception as e:
                    raise Exception("File: " + file['filename'] + ', Task: ' + task.task_id + ', ' + str(e))
                    assert False

if __name__ == '__main__':
    unittest.main()