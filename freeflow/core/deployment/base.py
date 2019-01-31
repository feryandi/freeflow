#!/usr/bin/python
# -*- coding: utf-8 -*-
#
try:
    import configparser
except Exception:
    import ConfigParser as configparser
import fnmatch
import glob
import json
import os

from six import string_types
from freeflow.core.log import Logged
from freeflow.core.security import decrypt


class BaseDeploy(Logged):
    """
    Base class for all deployment related classes.

    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, configuration):
        super(BaseDeploy, self).__init__()
        self.configuration = configuration

    def deploy(self):
        raise NotImplementedError('Deployer not implemented.')


class BaseRunner(Logged):
    """
    Base class for runner in different environment.
    """

    def __init__(self):
        super(BaseRunner, self).__init__()

    @staticmethod
    def run(args, configuration=None):
        raise NotImplementedError('Runner not implemented.')


class BaseRelocation(BaseDeploy):
    """
    Base class for folder relocation deployment.

    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, configuration):
        super(BaseRelocation, self).__init__(configuration)
        self.airflow_home = os.environ.get('AIRFLOW_HOME', '~/')

    @staticmethod
    def get_files_path(directory, file_pattern):
        matches = []
        for root, dirnames, filenames in os.walk(directory):
            for filename in fnmatch.filter(filenames, file_pattern):
                matches.append(os.path.join(root, filename))
        return matches


class BaseVariable(BaseDeploy):
    """
    Base class for variable deployment.

    :param path: variable file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(BaseVariable, self).__init__(configuration)
        self.path = path
        self.filename = os.path.basename(path)
        self.check()

    def check(self):
        with open(self.path, 'r') as file:
            f = file.read()

        try:
            json.loads(f)
        except Exception:
            raise IOError("Invalid JSON file at {}.".format(self.path))

        if not os.path.exists(self.path):
            raise IOError("Missing JSON file at {}.".format(self.path))


class BaseConfiguration(BaseDeploy):
    """
    Base class for Airflow configuration deployment.

    :param path: Airflow configuration file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(BaseConfiguration, self).__init__(configuration)
        self.airflow_home = os.environ.get('AIRFLOW_HOME', '~/')
        self.path = path
        self.config = self.__class__.read(self.path)

    @staticmethod
    def read(path):
        if not os.path.exists(path):
            raise IOError("Missing configuration file.")

        conf = configparser.ConfigParser()
        conf.read(path)
        return conf


class BaseConnection(BaseDeploy):
    """
    Base class for connection deployment.

    :param path: connection file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(BaseConnection, self).__init__(configuration)
        self.path = path
        self.encrypted = False
        self.data = None
        self.__check()
        self.__process()

    def __check(self):
        if not os.path.exists(self.path):
            raise IOError("Missing connection file.")

        if ".enc." in self.path:
            self.encrypted = True

        if self.encrypted:
            self.data = json.loads(decrypt(self.path))
        else:
            with open(self.path) as file:
                self.data = json.load(file)

    def __process(self):
        args = ['conn_id', 'conn_uri', 'conn_extra',
                'conn_type', 'conn_host', 'conn_login',
                'conn_password', 'conn_schema', 'conn_port']

        cmd = []
        for arg in args:
            if self.data.get(str(arg)) is not None:
                param = self.data.get(str(arg))
                if not isinstance(param, string_types):
                    param = json.dumps(param)

                cmd += ['--{}'.format(arg), str(param)]

        self.id = self.data.get('conn_id')
        self.args = cmd


class BasePool(BaseDeploy):
    """
    Base class for pool deployment.

    :param path: pool file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(BasePool, self).__init__(configuration)
        self.path = path
        self.data = None
        self.__check()
        self.__process()

    def __check(self):
        if not os.path.exists(self.path):
            raise IOError("Missing pool file.")

        with open(self.path) as file:
            self.data = json.load(file)

    def __process(self):
        args = ['name', 'slot_count', 'pool_description']

        cmd = []
        for arg in args:
            if self.data.get(str(arg)) is not None:
                param = self.data.get(str(arg))
                cmd += [str(param)]

        self.id = self.data.get('name')
        self.args = cmd


class BatchDeploy(Logged):
    """
    Helper class to do a batch deploy.

    :param folder_path: folder path where files for the
        deployment resides
    :type folder_path: str
    :param configuration: environment configuration
    :type configuration: dict
    :param deployer: type of class that will be deployed
    :type deployer: BaseDeploy
    """

    def __init__(self, folder_path, configuration, deployer):
        super(BatchDeploy, self).__init__()
        self.folder_path = folder_path
        self.configuration = configuration
        self.deployer = deployer
        self.__check()

    def __check(self):
        if not os.path.isdir(self.folder_path):
            raise IOError("Folder not found.")

    def deploy(self):
        for path in glob.glob("{}/*.json".format(self.folder_path)):
            self.log.debug("Processing file: {}".format(path))
            self.deployer(path, self.configuration).deploy()
