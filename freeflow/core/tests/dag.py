#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest

import freeflow.core.tests

from airflow import models as af_models


class DagTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = freeflow.core.tests.dag_files

    def test_dag_integrity(self):

        def check_valid_dag(dag):
            """
            Checks whether the python file is really a runnable DAG.

            :param dag: python module (file)
            :type dag: module
            """
            self.assertTrue(
                any(isinstance(var, af_models.DAG) for var in vars(dag).values()),
                "File does not contains a DAG instance"
            )

        def check_single_dag_file(dag_class):
            """
            Checks for count of the DAG in a single file. It should be
            only one.

            :param dag_class: list of DAG class instance
            :type dag_class: list(DAG)
            """
            self.assertTrue(
                len(dag_class) <= 1,
                "File should only contains a single DAG"
            )

        def check_dag_name(dag_class, filename):
            """
            Checks that DAG name should be snake case and same with the
            filename. If DAG versioning is needed, use <name>_v<number>

            :param dag_class: list of DAG class instance
            :type dag_class: list(DAG)
            :param filename: the filename which DAG class(es) resides
            :type filename: str
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
            """
            Checks uniqueness of task name within a DAG to ensure clarity

            :param task_class: list of task instance
            :type task_class: list(BaseOperator)
            """
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
