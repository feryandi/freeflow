#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import os

import freeflow.core.dag_loader as dag_loader

from airflow import models as af_models

class DagTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = dag_loader.get_dag_files()

    def test_dag_integrity(self):

        def check_valid_dag(dag):
            """ DAG Sanity Check """
            self.assertTrue(
                any(isinstance(var, af_models.DAG) for var in vars(dag).values()),
                "File does not contains a DAG instance"
            )

        def check_single_dag_file(dag_class):
            """ Single DAG on a File """
            self.assertTrue(
                len(dag_class) <= 1,
                "File should only contains a single DAG"
            )

        def check_dag_name(dag_class, filename):
            """
            DAG name should be snake case and same with the filename
            Exception: if need to do DAG versioning, use <name>_v<number>
            """
            dag_id = dag_class[0].dag_id
            self.assertEqual(
                dag_id.split('_v')[0],
                filename,
                "File name and DAG name should be the same"
            )
            self.assertTrue(
                all(c.islower() or c.isdigit() or c == '_' for c in dag_id),
                "DAG name should be all lower case"
            )

        def check_task_name_within_dag(task_class):
            """ Task name should be unique within a DAG to ensure clarity """
            tasks = task_class
            task_ids = []
            for task in tasks:
                task_ids.append(task.task_id)
                self.assertTrue(
                    all(c.islower() or c.isdigit() or c == '_' or c == '-' for c in task.task_id),
                    "Task name should be all lower case"
                )
            self.assertEqual(
                len(task_ids),
                len(set(task_ids)),
                "Task ID should not be duplicate"
            )

        for file in self._dag_files:
            check_valid_dag(file['dag'])
            check_single_dag_file(file['instance']['dags'])
            check_dag_name(file['instance']['dags'], file['filename'])
            check_task_name_within_dag(file['instance']['tasks'])


if __name__ == '__main__':
    unittest.main()
