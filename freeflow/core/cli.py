#!/usr/bin/python
# -*- coding: utf-8 -*-
try:
    import configparser
except Exception:
    import ConfigParser as configparser
import argparse
import os
import sys

CURRENT_WORKING_DIR = os.path.dirname(os.path.realpath('__file__'))
os.environ['AIRFLOW_HOME'] = CURRENT_WORKING_DIR

try:
    from flake8.main import cli as flake8
except ImportError:
    raise Exception(
        "Couldn't find Flake8. Are you sure it's installed?"
    )

try:
    import pytest
except ImportError:
    raise Exception(
        "Couldn't find PyTest. Are you sure it's installed?"
    )

from freeflow.core.log import Logged
from freeflow.core.initialization.direct import DirectInitialization
from freeflow.core.deployment.base import BatchDeploy
from freeflow.core.deployment.direct import (DirectRelocation,
                                             DirectVariable,
                                             DirectConfiguration,
                                             DirectConnection,
                                             DirectPool)
from freeflow.core.deployment.composer import (ComposerRelocation,
                                               ComposerVariable,
                                               ComposerConfiguration,
                                               ComposerConnection,
                                               ComposerPool)

import freeflow.core.tests


def clean():
    path = '{}/dags'.format(CURRENT_WORKING_DIR)
    files = os.listdir(path)
    for file in files:
        if file.endswith(".pyc"):
            os.remove(os.path.join(path, file))


def initialize(command):
    init = DirectInitialization(command.config, command.path['conf'])
    init.run()


def test(command):
    clean()  # Prevent pyc files to be considered as DAG

    if command.args.type == 'general':
        try:
            deploy(command, 'direct')
            freeflow.core.tests.run()
        except Exception:
            raise

    elif command.args.type == 'dags':
        deploy(command, 'direct')
        sys.path.append("{}/dags".format(os.environ.get('AIRFLOW_HOME')))
        raise SystemExit(pytest.main(['tests']))

    elif command.args.type is None:
        command.log.error("Please specify test type by using --type")

    else:
        command.log.error("Test type of '{}' not found"
                          .format(command.args.type))


def lint(command):
    additional_args = []
    if command.args.args is not None:
        additional_args = command.args.args.split(' ')
    flake8.main(['dags', 'tests'] + additional_args)


def deploy(command, deploy_type=None):
    clean()

    if deploy_type is None:
        deploy_type = command.args.type

    deploy = Deploy().get_classes(deploy_type)

    command.log.info("Applying folder relocation")
    deploy['rloc'](command.config).deploy()

    command.log.info("Applying variables")
    deploy['vars'](command.path['vars'], command.config).deploy()

    command.log.info("Applying configuration")
    deploy['conf'](command.path['conf'], command.config).deploy()

    command.log.info("Applying connection")
    BatchDeploy(command.path['conn'], command.config, deploy['conn']).deploy()

    command.log.info("Applying pool")
    BatchDeploy(command.path['pool'], command.config, deploy['pool']).deploy()


class Deploy(object):

    @staticmethod
    def get_classes(deploy_type):
        if deploy_type == 'composer':
            return {
                'rloc': ComposerRelocation,
                'vars': ComposerVariable,
                'conf': ComposerConfiguration,
                'conn': ComposerConnection,
                'pool': ComposerPool
            }
        else:
            return {
                'rloc': DirectRelocation,
                'vars': DirectVariable,
                'conf': DirectConfiguration,
                'conn': DirectConnection,
                'pool': DirectPool
            }


class Command(Logged):
    arguments = {
        'init': {
            'func': initialize,
            'desc': 'intialize Airflow',
            'args': []
        },
        'test': {
            'func': test,
            'desc': 'test the code and DAG',
            'args': ['type']
        },
        'lint': {
            'func': lint,
            'desc': 'lint the code',
            'args': ['args']
        },
        'deploy': {
            'func': deploy,
            'desc': 'deploy the code',
            'args': ['type']
        }
    }

    def __init__(self, argv=None):
        super(Command, self).__init__()
        self.args = self.__parser()
        self.path = {}
        self.config_path = '{}/conf/{}.cfg'.format(CURRENT_WORKING_DIR,
                                                   self.args.env)
        self.config = self.__read_config(self.config_path)

        airflow_data = ['conf', 'vars', 'conn', 'pool']

        try:
            for data in airflow_data:
                data_folder = self.config.get('imports', data)
                self.path[data] = "{}/data/{}/{}".format(CURRENT_WORKING_DIR,
                                                         data,
                                                         data_folder)
        except Exception as e:
            self.log.error("Configuration for importing '{}' data not found."
                           .format(data))
            raise Exception(e)

    def __parser(self):
        self.parser = argparse.ArgumentParser(
            description='Freeflow - an Airflow development tool.')

        self.parser.add_argument('--env', '-e',
                                 type=str,
                                 default='default',
                                 help='the Airflow environment '
                                      '(default: "%(default)s")')

        subparsers = self.parser.add_subparsers(dest='command', help='')

        for arg in self.arguments:
            subpar = subparsers.add_parser('{}'.format(arg),
                                           help=self.arguments[arg]['desc'])

            for narg in self.arguments[arg]['args']:
                subpar.add_argument('--{}'.format(narg))

        return self.parser.parse_args()

    def __read_config(self, path):
        conf = configparser.ConfigParser()
        conf.read(path)
        return conf

    def execute(self):
        try:
            command = self.arguments.get(self.args.command)
            if command is None:
                self.parser.print_help()
            else:
                command['func'](self)
        except Exception as e:
            self.log.error("{}".format(str(e).replace('\n', ' ')))
            raise


def execute(argv=None):
    command = Command(argv)
    command.execute()
