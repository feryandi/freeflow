#!/usr/bin/env python
"""Command line utility for development and deployment"""
import os
import sys
import subprocess

import ConfigParser

CWD = os.path.dirname(os.path.realpath('__file__'))
os.environ.setdefault('AIRFLOW_HOME', CWD)
os.environ.setdefault('ENV', 'default')

try:
  from airflow import settings
  from airflow.bin import cli
  from airflow.utils import db as db_utils
  from airflow.models import Variable
except ImportError:
  raise ImportError(
    "Couldn't find Airflow. Are you sure it's installed?"
  )

PATH_CONF = "{}/airflow/conf".format(CWD)
PATH_VARS = "{}/airflow/vars".format(CWD)
PATH_CONN = "{}/airflow/conn".format(CWD)
PATH_POOL = "{}/airflow/pool".format(CWD)

## TO-DO Core capability:
# - deploying: locally and composer (setup vars, conn, etc)
# - testing (using unit test)
# - lint
# - running the thing? running the thing!

CMD_CONFIG = None

def main():
  global CMD_CONFIG
  CMD_CONFIG = read_configuration('{}/conf/{}.cfg'.format(CWD, os.environ.get('ENV')))
  # initialize()

  # cli.initdb(None)

  # Linux and MacOS
  # command = ['./bin/freeflow.sh'] + sys.argv[1:]
  # process = subprocess.Popen(command, stderr=subprocess.PIPE)
  # out, err = process.communicate()


def initialize():
  global CMD_CONFIG
  db_utils.initdb()
  set_configuration("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf'))) # To handle example loading
  db_utils.resetdb()

def deploy():
  print("Applying variables")
  drop_variables()
  set_variables(CWD + '/airflow/vars/dev.json')

  print("Applying configuration")
  set_configuration("{}/{}".format(PATH_CONF, CMD_CONFIG.get('imports', 'conf')))

  print("TO-DO: Applying connection")
  print("TO-DO: Applying pool")
  print("TO-DO: Migrating DAG folder")

  print("Finished deploying changes.")
  print("Now, you could start the Airflow by running:")
  print("\tairflow webserver?")
  print("\tairflow worker?")

def drop_variables():
  session = settings.Session()
  session.query(Variable).delete()
  session.commit()
  session.close()

def set_variables(var_path):
  if os.path.exists(var_path):
    cli.import_helper(var_path)
  else:
    print("Missing variables file.")

def read_configuration(path):
  conf = ConfigParser.ConfigParser()
  conf.read(path)
  return conf

def set_configuration(path):
  if not os.path.exists(path):
    print("Missing configuraiton file.")

  current_conf_path = "{}/airflow.cfg".format(CWD)
  current_conf = read_configuration(current_conf_path)
  conf = read_configuration(path)
  for section in conf.sections():
    for (key, val) in conf.items(section):
      print(section, key, val)
      current_conf.set(section, key, val)

  with open(current_conf_path, 'wb') as configfile:
    current_conf.write(configfile)

if __name__ == '__main__':
  main()
