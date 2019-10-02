#!/usr/bin/python
# -*- coding: utf-8 -*-
#
try:
    import queue
except Exception:
    import Queue as queue
import subprocess
import threading

import freeflow.core.deployment.base as deployment

from google.cloud import storage


class ComposerRunner(deployment.BaseRunner):
    """
    Static runner class that could be called to run a CLI command
    within a Google Cloud Composer machine. This class also contains
    helper classes for Google Cloud related stuffs such as upload
    and deleting files from Google Cloud Storage.
    """

    def __init__(self):
        super(ComposerRunner, self).__init__()

    @staticmethod
    def upload(bucket_name, source, destination):
        gcs = storage.Client()
        bucket = gcs.get_bucket(bucket_name)
        blob = bucket.blob(destination)

        blob.upload_from_filename(source)

    @staticmethod
    def delete(bucket_name, prefix):
        def chunck(seq, size):
            return (seq[i::size] for i in range(size))

        gcs = storage.Client()
        bucket = gcs.get_bucket(bucket_name)
        blobs = [blob for blob in bucket.list_blobs(prefix=prefix)]
        blob_chuncks = list(chunck(blobs, int(len(blobs) / 1000) + 1))

        for chunk in blob_chuncks:
            if len(chunk) == 0:
                continue
            with gcs.batch():
                for blob in chunk:
                    blob.delete()

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
        cmd = ['config',
               'set',
               'project',
               configuration.get('composer', 'project_id')]
        ComposerRunner.gcloud(cmd)

        cmd = ['composer',
               'environments',
               'run',
               configuration.get('composer', 'name'),
               '--location',
               configuration.get('composer', 'location')] + args
        ComposerRunner.gcloud(cmd)


class ComposerRelocation(deployment.BaseRelocation):
    """
    Folder relocation deployment for Google Composer are being done
    by copying the local files to the Google Cloud Storage. This class
    will copy the folder `dags` and `data` to the respective folder
    with the same name in Google Cloud Storage.

    The uploading process is using threading to make sure the process
    is fast.

    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, configuration):
        super(ComposerRelocation, self).__init__(configuration)
        self.queue = queue.Queue()

    def worker(self):
        while True:
            try:
                file = self.queue.get()
                bucket = self.configuration.get('composer', 'bucket')
                gcs_target_path = file.replace(self.airflow_home + "/", "")

                ComposerRunner.upload(bucket,
                                      file,
                                      gcs_target_path)
            except Exception as e:
                self.log.error("Exception on thread: {}".format(e))
            finally:
                self.queue.task_done()

    def deploy(self):
        folders = ["dags", "data"]

        for folder in folders:
            self.log.info("Uploading '{}' folder to GCS bucket..."
                          .format(folder))
            self.log.info("Cleaning up the '{}' folder in GCS"
                          .format(folder))

            ComposerRunner.delete(self.configuration.get('composer', 'bucket'),
                                  '{}/'.format(folder))

            files = self.__class__.get_files_path("{}/{}"
                                                  .format(self.airflow_home,
                                                          folder),
                                                  "*")

            for i in range(int(len(files) / 10)):
                t = threading.Thread(target=self.worker)
                t.daemon = True
                t.start()

            self.log.info("Sending upload request for {} file(s)"
                          .format(len(files)))
            for file in files:
                self.queue.put(file)

            self.queue.join()
            self.log.info("Done uploading '{}' folder to GCS bucket"
                          .format(folder))


class ComposerVariable(deployment.BaseVariable):
    """
    Deploy the variable in composer deployment mode which runs the
    Airflow import CLI command.

    :param path: variable file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(ComposerVariable, self).__init__(path, configuration)

    def deploy(self):
        path = '/home/airflow/gcs/data/vars/{}'.format(self.filename)
        cmd = ['variables', '--', '-i', path]
        self.log.debug("Importing variables from file: {}".format(path))

        ComposerRunner.run(cmd, self.configuration)
        self.log.info("Successfully updated variables")


class ComposerConfiguration(deployment.BaseConfiguration):
    """
    Empty implementation of configuration deployment for the composer.
    This is not implemented because it is safer to do the configuration
    overriding via the Composer dashboard until Google gives API to do so.

    :param path: Airflow configuration file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(ComposerConfiguration, self).__init__(path, configuration)

    def deploy(self):
        self.log.warning("IMPORTANT. PLEASE READ.")
        self.log.warning("Cloud Composer haven't yet had API "
                         "that could override the configuration.")
        self.log.warning("Please override your configuration via "
                         "Google Cloud Composer dashboard.")


class ComposerConnection(deployment.BaseConnection):
    """
    Deploy the connection by deleting it first, and the adding (or re-adding)
    the connection via the CLI command.

    :param path: connection file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(ComposerConnection, self).__init__(path, configuration)

    def deploy(self):
        cmd = ['connections', '--', '-d', '--conn_id', self.id]
        ComposerRunner.run(cmd, self.configuration)

        cmd = ['connections', '--', '-a'] + self.args
        ComposerRunner.run(cmd, self.configuration)

        self.log.info("Added connection with name: {}".format(self.id))


class ComposerPool(deployment.BasePool):
    """
    Deploy the pool by deleting it first, and the adding (or re-adding)
    the pool via the CLI command.

    :param path: pool file path
    :type path: str
    :param configuration: environment configuration
    :type configuration: dict
    """

    def __init__(self, path, configuration):
        super(ComposerPool, self).__init__(path, configuration)

    def deploy(self):
        cmd = ['pool', '--', '-x', self.id]
        ComposerRunner.run(cmd, self.configuration)

        cmd = ['pool', '--', '-s'] + self.args
        ComposerRunner.run(cmd, self.configuration)

        self.log.info("Added pool '{}'.".format(self.id))
