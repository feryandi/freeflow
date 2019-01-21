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

import freeflow.test

from freeflow.core.initialization.direct import DirectInitialization
from freeflow.core.deployment.direct import (DirectVariable, DirectConfiguration, DirectConnection, DirectPool)

# TO-DO: environment choice (based on conf files)

def clean():
  path = '{}/dags'.format(CURRENT_WORKING_DIR)
  files = os.listdir(path)
  for file in files:
    if file.endswith(".pyc"):
      os.remove(os.path.join(path, file))

def helper(command):
  print("Available commands:")
  for argument in command.arguments:
    print("  {:<8}\t\t{}".format(argument, command.arguments[argument]['desc']))

def initialize(command):
  init = DirectInitialization(command.path_conf)
  init.run()

def test(command):
  try:
    clean() # Prevent pyc files to be considered as DAG
    deploy(command)
    freeflow.test.run()
  except Exception as e:
    print(e)

def lint(command):
  flake8.main(['dags', 'tests'])

def deploy(command):
  print("Applying variables")
  d = DirectVariable()
  d.drop()
  d.set(command.path_vars)

  print("Applying configuration")
  d = DirectConfiguration()
  d.set(command.path_conf)

  print("Applying connection")
  d = DirectConnection()
  d.batch(command.path_conn)

  print("Applying pool")
  d = DirectPool()
  d.batch(command.path_pool)

  print("Done")


class Command(object):
  arguments = {
    'help': {
      'func': helper,
      'desc': 'Shows available commands'
    },
    'init': {
      'func': initialize,
      'desc': 'Intialize Airflow'
    },
    'test': {
      'func': test,
      'desc': 'Test the code and DAG'
    },
    'lint': {
      'func': lint,
      'desc': 'Lint the code'
    },
    'deploy': {
      'func': deploy,
      'desc': 'Deploy the code',
      'args': ['']
    }
  }

  def __init__(self, argv=None):
    self.argv = argv or sys.argv[:]
    self.config_path = '{}/conf/{}.cfg'.format(CURRENT_WORKING_DIR,
                                               os.environ.get('ENV', 'default'))
    self.config = self.__read_config(self.config_path)

    self.path_conf = "{}/airflow/conf/{}".format(CURRENT_WORKING_DIR,
                                                 self.config.get('imports', 'conf'))
    self.path_vars = "{}/airflow/vars/{}".format(CURRENT_WORKING_DIR,
                                                 self.config.get('imports', 'vars'))
    self.path_conn = "{}/airflow/conn/{}".format(CURRENT_WORKING_DIR,
                                                 self.config.get('imports', 'conn'))
    self.path_pool = "{}/airflow/pool/{}".format(CURRENT_WORKING_DIR,
                                                 self.config.get('imports', 'pool'))

  def __read_config(self, path):
    conf = ConfigParser.ConfigParser()
    conf.read(path)
    return conf

  def execute(self):
    try:
      command = self.arguments[self.argv[1]]
      command['func'](self)
    except Exception as e:
      helper(self)


def execute(argv=None):
  command = Command(argv)
  command.execute()
