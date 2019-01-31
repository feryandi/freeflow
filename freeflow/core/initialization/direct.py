#!/usr/bin/python
# -*- coding: utf-8 -*-
#
try:
    from airflow.utils import db
except ImportError:
    raise Exception(
        "Couldn't find Airflow. Are you sure it's installed?"
    )

from freeflow.core.deployment.direct import DirectConfiguration


class DirectInitialization(object):
    """
    Initialize Airflow directly via importing the Airflow library.
    This will initialize the database, importing the Airflow config
    and replace the default one, then re-initialize the database.
    (Re-initialization is important when trying to get rid example
    DAGs)

    :param configuration: environment configuration (from conf/*.cfg)
    :type configuration: dict
    :param airflow_config: path of the user-defined Airflow config
        to replace the default Airflow configuration.
    :type airflow_config: str
    """

    def __init__(self, configuration, airflow_config):
        self.configuration = configuration
        self.airflow_config = airflow_config

    def run(self):
        config = DirectConfiguration(self.airflow_config, self.configuration)
        config.deploy()
        db.initdb()
