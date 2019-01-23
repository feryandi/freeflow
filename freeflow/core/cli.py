import argparse
import ConfigParser
import os
import sys

CURRENT_WORKING_DIR = os.path.dirname(os.path.realpath('__file__'))
os.environ.setdefault('AIRFLOW_HOME', CURRENT_WORKING_DIR)

try:
  from flake8.main import cli as flake8
except ImportError:
  raise ImportError(
    "Couldn't find Flake8. Are you sure it's installed?"
  )

try:
  import pytest
except ImportError:
  raise ImportError(
    "Couldn't find PyTest. Are you sure it's installed?"
  )

from freeflow.core.log import Logged
from freeflow.core.initialization.direct import DirectInitialization
from freeflow.core.deployment.direct import (DirectVariable, DirectConfiguration, DirectConnection, DirectPool)

import freeflow.test


def clean():
  path = '{}/dags'.format(CURRENT_WORKING_DIR)
  files = os.listdir(path)
  for file in files:
    if file.endswith(".pyc"):
      os.remove(os.path.join(path, file))


def initialize(command):
  init = DirectInitialization(command.path['conf'])
  init.run()


def test(command):
  clean() # Prevent pyc files to be considered as DAG
  if command.args.type == 'general':
    try:
      deploy(command)
      freeflow.test.run()
    except Exception as e:
      command.log.error(e)

  elif command.args.type == 'dags':
    sys.path.append("{}/dags".format(os.environ.get('AIRFLOW_HOME')))
    pytest.main(['tests'])

  elif command.args.type is None:
    command.log.error("Please specify test type by using --type")
  
  else:
    command.log.error("Test type of '{}' not found".format(command.args.type))


def lint(command):
  flake8.main(['dags', 'tests'])


def deploy(command):
  command.log.info("Applying variables")
  d = DirectVariable()
  d.drop()
  d.set(command.path['vars'])

  command.log.info("Applying configuration")
  d = DirectConfiguration()
  d.set(command.path['conf'])

  command.log.info("Applying connection")
  d = DirectConnection()
  d.batch(command.path['conn'])

  command.log.info("Applying pool")
  d = DirectPool()
  d.batch(command.path['pool'])

  command.log.warn("Migrating DAG (plugins, data?) folder")


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
      'args': []
    },
    'deploy': {
      'func': deploy,
      'desc': 'deploy the code',
      'args': []
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
        self.path[data] = "{}/data/{}/{}".format(CURRENT_WORKING_DIR, data,
                                                   self.config.get('imports', data))
    except Exception as e:
      self.log.error("Configuration for importing '{}' data not found.".format(data))
      raise Exception(e)

  def __parser(self):
    self.parser = argparse.ArgumentParser(description='Freeflow - an Airflow development tool.')
    self.parser.add_argument('--env', '-e',
                             type=str,
                             default='default',
                             help='the Airflow environment (default: "%(default)s")')

    subparsers = self.parser.add_subparsers(dest='command', help='')

    for arg in self.arguments:
      subpar = subparsers.add_parser('{}'.format(arg), help=self.arguments[arg]['desc'])

      for narg in self.arguments[arg]['args']:
        subpar.add_argument('--{}'.format(narg))

    return self.parser.parse_args()

  def __read_config(self, path):
    conf = ConfigParser.ConfigParser()
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
      raise Exception(e)


def execute(argv=None):
  command = Command(argv)
  command.execute()
