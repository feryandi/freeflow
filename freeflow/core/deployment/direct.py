import ConfigParser
import glob
import os
import json

from freeflow.core.security import decrypt
from freeflow.core.log import (SuppressPrints, Logged)

try:
  from airflow.bin import cli
  from airflow import settings
  from airflow.models import Variable
except ImportError:
  raise ImportError(
    "Couldn't find Airflow. Are you sure it's installed?"
  )

class DirectDeploy(Logged):

  def __init__(self):
    super(DirectDeploy, self).__init__()
    self._airflow_home = os.environ.get('AIRFLOW_HOME', '~/')

  def run(self, args):
    with SuppressPrints():
      parser = cli.get_parser()
      args = parser.parse_args(args)
      args.func(args)

  def batch(self, path):
    raise NotImplemented('Batch set not yet implemented for this setup.')

  def set(self, path):
    raise NotImplemented('Set not yet implemented for this setup.')

  def delete(self, key):
    raise NotImplemented('Delete not yet implemented for this setup.')

  def drop(self):
    raise NotImplemented('Drop not yet implemented for this setup.')


class DirectVariable(DirectDeploy):

  def __init__(self):
    super(DirectVariable, self).__init__()

  def __check_json(self, path):
    with open(path, 'r') as varfile:
      var = varfile.read()

    try:
      d = json.loads(var)
    except Exception:
      raise IOError("Invalid variables file.")

  def set(self, path):
    if not os.path.exists(path):
      raise IOError("Missing variables file.")
    self.__check_json(path)

    cmd = ['variables', '-i', path]
    self.log.debug("Importing variables from file: {}".format(path))
    super(DirectVariable, self).run(cmd)
    self.log.info("Successfully updated variables")

  def drop(self):
    session = settings.Session()
    session.query(Variable).delete()
    session.commit()
    session.close()


class DirectConfiguration(DirectDeploy):

  def __init__(self):
    super(DirectConfiguration, self).__init__()

  def read(self, path):
    if not os.path.exists(path):
      raise IOError("Missing configuration file.")

    conf = ConfigParser.ConfigParser()
    conf.read(path)
    return conf

  def set(self, path):
    airflow_home = self._airflow_home
    # TO-DO: is it always airflow.cfg?
    airflow_config_path = "{}/airflow.cfg".format(airflow_home)
    airflow_config = self.read(airflow_config_path)

    imported_config = self.read(path)

    for section in imported_config.sections():
      for (key, val) in imported_config.items(section):
        airflow_config.set(section, key, val)
        self.log.info("Added [{}] {} = {}".format(section, key, val))

    with open(airflow_config_path, 'wb') as config_file:
      airflow_config.write(config_file)


class DirectConnection(DirectDeploy):

  def __init__(self):
    super(DirectConnection, self).__init__()

  def batch(self, path):
    if not os.path.isdir(path):
      raise IOError("Connection folder not found.")

    for file in glob.glob("{}/*.json".format(path)):
      self.log.debug("Processing connection file: {}".format(file))
      self.set(file)

  def set(self, path):
    if not os.path.exists(path):
      raise IOError("Missing connection file.")

    encrypted = False
    if ".enc." in path:
      encrypted = True

    data = None

    if encrypted:
      data = json.loads(decrypt(path))
    else:
      with open(path) as file:
        data = json.load(file)

    cmd = ['connections', '-a']

    available_args = ['conn_id', 'conn_uri', 'conn_extra',
                      'conn_type', 'conn_host', 'conn_login',
                      'conn_password', 'conn_schema', 'conn_port']

    for arg in available_args:
      if data.get(str(arg)) is not None:
        param = data.get(str(arg))
        if not isinstance(param, basestring):
          param = json.dumps(param)

        cmd += ['--{}'.format(arg), str(param)]

    self.delete(data.get('conn_id'))
    super(DirectConnection, self).run(cmd)

    self.log.info("Added '{}' connection with name: {}".format(data.get('conn_type'), data.get('conn_id')))

  def delete(self, key):
    cmd = ['connections', '-d', '--conn_id', key]
    super(DirectConnection, self).run(cmd)


class DirectPool(DirectDeploy):

  def __init__(self):
    super(DirectPool, self).__init__()

  def batch(self, path):
    if not os.path.isdir(path):
      raise IOError("Pool folder not found.")

    for file in glob.glob("{}/*.json".format(path)):
      self.log.debug("Processing pool file: {}".format(file))
      self.set(file)

  def set(self, path):
    if not os.path.exists(path):
      raise IOError("Missing pool file.")

    with open(path) as file:
      data = json.load(file)
      cmd = ['pool', '-s']

      available_args = ['name', 'slot_count', 'pool_description']

      for arg in available_args:
        if data.get(str(arg)) is not None:
          param = data.get(str(arg))
          cmd += [str(param)]

      self.delete(data.get('name'))
      super(DirectPool, self).run(cmd)
      self.log.info("Added pool '{}' with {} slot(s).".format(data.get('name'), data.get('slot_count')))

  def delete(self, key):
    cmd = ['pool', '-x', key]
    super(DirectPool, self).run(cmd)
