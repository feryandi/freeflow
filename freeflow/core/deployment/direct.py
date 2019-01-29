#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import freeflow.core.deployment.base as deployment

from freeflow.core.log import SuppressPrints

try:
    from airflow.bin import cli
    from airflow import settings
    from airflow.models import Variable
except ImportError:
    raise ImportError(
        "Couldn't find Airflow. Are you sure it's installed?"
        "install: `pip install apache-airflow==1.9.0`"
    )


class DirectRunner(deployment.BaseRunner):
    """
    Static runner class that could be called to run a CLI command
    directly to the installed Airflow in local machine.
    """

    def __init__(self):
        super(DirectRunner, self).__init__()

    @staticmethod
    def run(args, configuration=None):
        with SuppressPrints():
            parser = cli.get_parser()
            args = parser.parse_args(args)
            args.func(args)


class DirectRelocation(deployment.BaseRelocation):
    """
    Empty implementation of folder relocation for the direct deploy.
    This is not implemented because it is assumed that the folders
    already always in place to be able to run the Airflow.

    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, configuration):
        super(DirectRelocation, self).__init__(configuration)

    def deploy(self):
        self.log.warning("No folder relocation done "
                         "on direct Airflow deployment.")


class DirectVariable(deployment.BaseVariable):
    """
    Deploy the variable in direct deployment mode which runs the
    Airflow import CLI command.

    :param path: variable file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(DirectVariable, self).__init__(path, configuration)

    def deploy(self):
        session = settings.Session()
        session.query(Variable).delete()
        session.commit()
        session.close()

        cmd = ['variables', '-i', self.path]
        self.log.debug("Importing variables from file: {}".format(self.path))

        DirectRunner.run(cmd)
        self.log.info("Successfully updated variables")


class DirectConfiguration(deployment.BaseConfiguration):
    """
    Deploy the configuration by replacing the default configuration
    value with the given one. If there is no replacer, then the default
    value are being kept.

    :param path: Airflow configuration file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(DirectConfiguration, self).__init__(path, configuration)

    def deploy(self):
        # TO-DO: is it always airflow.cfg?
        airflow_config_path = "{}/airflow.cfg".format(self.airflow_home)
        airflow_config = self.__class__.read(airflow_config_path)

        for section in self.config.sections():
            for (key, val) in self.config.items(section):
                try:
                    airflow_config.add_section(section)
                except:
                    pass
                finally:
                    airflow_config.set(section, key, val)
                self.log.info("Added [{}] {} = {}".format(section, key, val))

        with open(airflow_config_path, 'w') as config_file:
            airflow_config.write(config_file)


class DirectConnection(deployment.BaseConnection):
    """
    Deploy the connection by deleting it first, and the adding (or re-adding)
    the connection via the CLI command.

    :param path: connection file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(DirectConnection, self).__init__(path, configuration)

    def deploy(self):
        cmd = ['connections', '-d', '--conn_id', self.id]
        DirectRunner.run(cmd)

        cmd = ['connections', '-a'] + self.args
        DirectRunner.run(cmd)

        self.log.info("Added connection with name: {}".format(self.id))


class DirectPool(deployment.BasePool):
    """
    Deploy the pool by deleting it first, and the adding (or re-adding)
    the pool via the CLI command.

    :param path: pool file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(DirectPool, self).__init__(path, configuration)

    def deploy(self):
        cmd = ['pool', '-x', self.id]
        DirectRunner.run(cmd)

        cmd = ['pool', '-s'] + self.args
        DirectRunner.run(cmd)

        self.log.info("Added pool '{}'.".format(self.id))
