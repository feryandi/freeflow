#!/usr/bin/python
# -*- coding: utf-8 -*-
try:
    from airflow.utils import db
except ImportError:
    raise ImportError(
        "Couldn't find Airflow. Are you sure it's installed?"
    )

from freeflow.core.deployment.direct import DirectConfiguration


class DirectInitialization(object):

    def __init__(self, configuration, airflow_config):
        self.configuration = configuration
        self.airflow_config = airflow_config

    def run(self):
        db.initdb()
        config = DirectConfiguration(self.airflow_config, self.configuration)
        config.deploy()
        db.resetdb()
