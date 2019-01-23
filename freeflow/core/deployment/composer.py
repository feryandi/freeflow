import subprocess

from freeflow.core.log import SuppressPrints
import freeflow.core.deployment.base as deployment

from google.cloud import storage


class ComposerRunner(deployment.BaseRunner):

  def __init__(self):
    super(ComposerRunner, self).__init__()

  @staticmethod
  def upload(bucket_name, source, destination):
    gcs = storage.Client()
    bucket = gcs.get_bucket(bucket_name)
    blob = bucket.blob(destination)

    blob.upload_from_filename(source)

  @staticmethod
  def gcloud(args):
    cmd = ['gcloud'] + args

    try:
      output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exception:
      raise RuntimeError(exception.output)
    except OSError as exception:
      print("Couldn't find gcloud. Are you sure it's installed?")
      raise exception
    else:
      print(output)

  @staticmethod
  def run(args, configuration=None):
    # with SuppressPrints():
    cmd = ['config', 'set', 'project', configuration.get('composer', 'project_id')]
    ComposerRunner.gcloud(cmd)

    cmd = ['composer', 
           'environments',
           'run',
           configuration.get('composer', 'name'),
           '--location',
           configuration.get('composer', 'location')] + args
    ComposerRunner.gcloud(cmd)


class ComposerVariable(deployment.BaseVariable):

  def __init__(self, path, configuration):
    super(ComposerVariable, self).__init__(path, configuration)

  def deploy(self):
    ComposerRunner.upload(self.configuration.get('composer', 'bucket'),
                          self.path,
                          "data/vars/{}".format(self.filename))

    path = '/home/airflow/gcs/data/vars/{}'.format(self.filename)
    cmd = ['variables', '--', '-i', path]
    self.log.debug("Importing variables from file: {}".format(path))

    ComposerRunner.run(cmd, self.configuration)
    self.log.info("Successfully updated variables")


class ComposerConfiguration(deployment.BaseConfiguration):

  def __init__(self, path, configuration):
    super(ComposerConfiguration, self).__init__(path, configuration)

  def deploy(self):
    self.log.warning("IMPORTANT. PLEASE READ.")
    self.log.warning("Cloud Composer haven't yet had API that could override the configuration.")
    self.log.warning("Please override your configuration via Google Cloud Composer dashboard.")


class ComposerConnection(deployment.BaseConnection):

  def __init__(self, path, configuration):
    super(ComposerConnection, self).__init__(path, configuration)

  def deploy(self):
    cmd = ['connections', '--', '-d', '--conn_id', self.id]
    ComposerRunner.run(cmd, self.configuration)

    cmd = ['connections', '--', '-a'] + self.args
    ComposerRunner.run(cmd, self.configuration)

    self.log.info("Added connection with name: {}".format(self.id))


class ComposerPool(deployment.BasePool):

  def __init__(self, path, configuration):
    super(ComposerPool, self).__init__(path, configuration)

  def deploy(self):
    cmd = ['pool', '--', '-x', self.id]
    ComposerRunner.run(cmd, self.configuration)

    cmd = ['pool', '--', '-s'] + self.args
    ComposerRunner.run(cmd, self.configuration)

    self.log.info("Added pool '{}'.".format(self.id))