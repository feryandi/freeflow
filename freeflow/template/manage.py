#!/usr/bin/env python
"""Command line utility for development and deployment"""
import glob
import os
import sys
import subprocess
import unittest
import json

import ConfigParser

CWD = os.path.dirname(os.path.realpath('__file__'))
os.environ.setdefault('AIRFLOW_HOME', CWD)
os.environ.setdefault('ENV', 'default')

try:
  import freeflow.test
  from freeflow.core.deployment.direct import (DirectVariable, DirectConfiguration, DirectConnection, DirectPool)
except ImportError as e:
  print(e)
  raise ImportError(
    "Couldn't find Freeflow. Are you sure it's installed?"
  )

try:
  from airflow.bin import cli
  from airflow.utils import db
except ImportError:
  raise ImportError(
    "Couldn't find Airflow. Are you sure it's installed?"
  )

try:
  from flake8.main import cli as flake8
except ImportError:
  raise ImportError(
    "Couldn't find Flake8. Are you sure it's installed?"
  )

PATH_CONF = "{}/airflow/conf".format(CWD)
PATH_VARS = "{}/airflow/vars".format(CWD)
PATH_CONN = "{}/airflow/conn".format(CWD)
PATH_POOL = "{}/airflow/pool".format(CWD)

## TO-DO Core capability:
# - deploying: locally and composer (setup vars, conn, etc)
# - running the thing? running the thing!

CMD_CONFIG = None

def main():
  global CMD_CONFIG
  CMD_CONFIG = read_configuration('{}/conf/{}.cfg'.format(CWD, os.environ.get('ENV')))
  # test()
  # lint()
  initialize()
  deploy()
  # encrypt()

  # decrypt('{}/airflow/conn/default/test_default.enc.json'.format(CWD))

  # set_connections('./airflow/conn/default/test_default.json')

  # initialize()

  # cli.initdb(None)

  # Linux and MacOS
  # command = ['./bin/freeflow.sh'] + sys.argv[1:]
  # process = subprocess.Popen(command, stderr=subprocess.PIPE)
  # out, err = process.communicate()

def encrypt():
  print('Use sops to encrypt the file.')
  print('Learn more at https://github.com/mozilla/sops')

def decrypt(path):
  print(path)
  cmd = ['sops', '-d', path]
  try:
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as e:
    print(e.output)
  except OSError as e:
    print("Couldn't find sops. Are you sure it's installed?")
  else:
    return output

def test():
  # tests = unittest.TestLoader().discover(start_dir='tests')
  freeflow.test.run()

def lint():
  # try:
  flake8.main(['dags', 'tests'])
  # except SystemExit as e:
  #   print(e)

def initialize():
  global CMD_CONFIG
  db.initdb()

  d = DirectConfiguration()
  d.set("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf')))
  # set_configuration("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf'))) # To handle example loading
  db.resetdb()

def deploy():
  print("Applying variables")
  d = DirectVariable()
  d.drop()
  d.set("{}/{}".format(PATH_VARS, CMD_CONFIG.get('imports', 'var')))
  # drop_variables()
  # set_variables("{}/{}".format(PATH_VARS, CMD_CONFIG.get('imports', 'var')))

  print("Applying configuration")
  d = DirectConfiguration()
  d.set("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf')))
  # set_configuration("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf')))

  print("Applying connection")
  d = DirectConnection()
  d.batch("{}/{}".format(PATH_CONN, CMD_CONFIG.get('imports', 'conn')))
  # set_connections("{}/{}".format(PATH_CONN, CMD_CONFIG.get('imports', 'conn')))

  print("Applying pool")
  d = DirectPool()
  d.batch("{}/{}".format(PATH_POOL, CMD_CONFIG.get('imports', 'pool')))
  # set_pools("{}/{}".format(PATH_POOL, CMD_CONFIG.get('imports', 'conn')))

  print("TO-DO: Migrating DAG folder")

  print("Finished deploying changes.")
  print("Now, you could start the Airflow by running:")
  print("\tairflow webserver?")
  print("\tairflow worker?")


def read_configuration(path):
  conf = ConfigParser.ConfigParser()
  conf.read(path)
  return conf


if __name__ == '__main__':
  main()
