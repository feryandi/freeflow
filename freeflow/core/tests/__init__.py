#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import unittest

from freeflow.core.tests.dag import DagTest
from freeflow.core.tests.sensor.externaltask import SensorExternalTaskTest

# Optional tests
try:
    from freeflow.core.tests.operator.bigquery import OperatorBigqueryTest
except ImportError:
    pass

from freeflow.core.dag_loader import get_dag_files

test_classes = [DagTest,
                SensorExternalTaskTest]

# Optional tests
try:
    test_classes += [OperatorBigqueryTest]
except Exception:
    pass

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
    result = test_runner.run(test_suites)

    if len(result.failures) > 0 or len(result.errors) > 0:
        raise SystemExit(1)
