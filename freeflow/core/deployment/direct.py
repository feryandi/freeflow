import freeflow.core.deployment.base as deployment

from freeflow.core.log import (SuppressPrints, Logged)

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

  def __init__(self):
    super(DirectRunner, self).__init__()

  @staticmethod
  def run(args, configuration=None):
    with SuppressPrints():
      parser = cli.get_parser()
      args = parser.parse_args(args)
      args.func(args)


class DirectVariable(deployment.BaseVariable):

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

  def __init__(self, path, configuration):
    super(DirectConfiguration, self).__init__(path, configuration)

  def deploy(self):
    # TO-DO: is it always airflow.cfg?
    airflow_config_path = "{}/airflow.cfg".format(self.airflow_home)
    airflow_config = self.__class__.read(airflow_config_path)

    for section in self.config.sections():
      for (key, val) in self.config.items(section):
        airflow_config.set(section, key, val)
        self.log.info("Added [{}] {} = {}".format(section, key, val))

    with open(airflow_config_path, 'wb') as config_file:
      airflow_config.write(config_file)


class DirectConnection(deployment.BaseConnection):

  def __init__(self, path, configuration):
    super(DirectConnection, self).__init__(path, configuration)

  def deploy(self):
    cmd = ['connections', '-d', '--conn_id', self.id]
    DirectRunner.run(cmd)

    cmd = ['connections', '-a'] + self.args
    DirectRunner.run(cmd)

    self.log.info("Added connection with name: {}".format(self.id))


class DirectPool(deployment.BasePool):

  def __init__(self, path, configuration):
    super(DirectPool, self).__init__(path, configuration)

  def deploy(self):
    cmd = ['pool', '-x', self.id]
    DirectRunner.run(cmd)

    cmd = ['pool', '-s'] + self.args
    DirectRunner.run(cmd)

    self.log.info("Added pool '{}'.".format(self.id))
