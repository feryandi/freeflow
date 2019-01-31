#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import io
import os
import sys
import logging
try:
    import StringIO
except ImportError:
    pass

try:
    from airflow.utils.log.logging_mixin import LoggingMixin
except ImportError:
    raise Exception(
        "Couldn't find Airflow. Are you sure it's installed?"
    )

import lib2to3.pgen2.driver


class FetchPrints(object):
    """
    Fetch printed value into a string.
    """
    def __enter__(self):
        self._original_stdout = sys.stdout
        try:
            sys.stdout = buffer = StringIO.StringIO()
        except Exception:
            sys.stdout = buffer = io.StringIO()
        return buffer

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout


class SuppressPrints(object):
    """
    The class is a context manager class that could suppress any
    prints that being called within the block. This functionality
    is needed when there is third-party logging nor prints that
    obscuring actual log.
    """
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


class Logged(object):
    """
    Base class that give ability for other classes to do logging
    by simple call `self.log` and make the logger consistent across
    project.
    """
    def __init__(self):
        self.log = logging.getLogger(str(__name__).split('.')[0])


class Lib2to3Logging(object):
    """
    Helper class to override the lib2to3 logger
    """
    def getLogger(self):
        return logging.getLogger('lib2to3')


# Overriding lib2to3 logger
lib2to3.pgen2.driver.logging = Lib2to3Logging()

# Suppress lib2to3 log to only shows when there is an `ERROR`
logging.getLogger('lib2to3').setLevel(logging.ERROR)

# Supress Airflow log to only shows when there is an `ERROR`
LoggingMixin().log.setLevel(logging.ERROR)
