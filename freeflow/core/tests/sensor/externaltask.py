#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest

import freeflow.core.tests

from airflow.operators.sensors import ExternalTaskSensor


class SensorExternalTaskTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._dag_files = freeflow.core.tests.dag_files

    def test_sensor_external_task(self):

        def check_depend_itself(current_dag, current_task):
            """ Prevent the sensor to depend on itself """
            is_current_dag = current_task.external_dag_id == current_dag
            is_current_task = \
                current_task.external_task_id == current_task.task_id
            if is_current_dag and is_current_task:
                raise Exception('Cannot depend on itself')

        def check_external_dag_availability(current_task, dags_available):
            """ Prevent the sensor to depend on unavailable DAG """
            if current_task.external_dag_id not in dags_available:
                raise Exception('DAG named {} is not available'
                                .format(current_task.external_dag_id))

        def check_external_task_availability(current_task,
                                             dags_available,
                                             dag_files):
            """
            Prevent the sensor to depend on unavailable task within a DAG
            """
            dag_index = dags_available.index(current_task.external_dag_id)
            dag_tasks = dag_files[dag_index]['instance']['tasks']
            tasks_list = [task.task_id for task in dag_tasks]

            if current_task.external_task_id not in tasks_list:
                raise Exception('Task named {} is not available in DAG {}'
                                .format(current_task.external_task_id,
                                        current_task.external_dag_id))
            return dag_tasks[tasks_list.index(current_task.external_task_id)]

        def check_valid_execution_value(current_task, external_task):
            """
            Check whether the specified execution date could be achieved
            by the external task
            TO-DO: need to find formula to check whether two arithmetic
            progression is within another (not only intersect)
            """
            None

        dags_available = [d['instance']['dags'][0].dag_id for d in self._dag_files]
        for file in self._dag_files:
            for task in file['instance']['tasks']:
                try:
                    if isinstance(task, ExternalTaskSensor):
                        current_dag = file['instance']['dags'][0].dag_id
                        check_depend_itself(current_dag, task)
                        check_external_dag_availability(task,
                                                        dags_available)
                        check_external_task_availability(task,
                                                         dags_available,
                                                         self._dag_files)
                        # check_valid_execution_value(task, external_task)
                except Exception as e:
                    raise Exception("File: {}, Task: {}, {}"
                                    .format(file['filename'],
                                            task.task_id,
                                            str(e)))
                    self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
