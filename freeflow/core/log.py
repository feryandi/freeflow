import os
import sys
import logging

from inspect import getframeinfo, stack

class SuppressPrints(object):
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

class Logged(object):
    def __init__(self):
        self.log = logging.getLogger(str(__name__).split('.')[0])

# This is to suppress informational logging from Airflow and lib2to3
import lib2to3.pgen2.driver

class Lib2to3Logging(object):
    def getLogger(self):
        return logging.getLogger('lib2to3')

lib2to3.pgen2.driver.logging = Lib2to3Logging()
logging.getLogger('lib2to3').setLevel(logging.ERROR)

from airflow.utils.log.logging_mixin import LoggingMixin

LoggingMixin().log.setLevel(logging.ERROR)
