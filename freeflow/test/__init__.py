import unittest

from freeflow.test.dag import DagTest
from freeflow.test.operator.bigquery import OperatorBigqueryTest
from freeflow.test.sensor.externaltask import SensorExternalTaskTest

from freeflow.core.dag_loader import get_dag_files

test_classes = [
                                DagTest,
                                OperatorBigqueryTest,
                                SensorExternalTaskTest
                             ]

dag_files = []


def run():
    global dag_files
    dag_files = get_dag_files()

    test_loader = unittest.TestLoader()

    suites = []
    for test_class in test_classes:
            suite = test_loader.loadTestsFromTestCase(test_class)
            suites.append(suite)

    test_suites = unittest.TestSuite(suites)
    test_runner = unittest.TextTestRunner()
    test_runner.run(test_suites)
