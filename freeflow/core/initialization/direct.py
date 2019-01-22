import ConfigParser
import glob
import os

try:
  from airflow.utils import db
except ImportError:
  raise ImportError(
    "Couldn't find Airflow. Are you sure it's installed?"
  )

from freeflow.core.deployment.direct import DirectConfiguration

class DirectInitialization(object):

  def __init__(self, config_path):
    if config_path is None:
      raise AttributeError('Missing initialization configuration path')
    self.config_path = config_path

  def run(self):    
    db.initdb()
    config = DirectConfiguration()
    config.set(self.config_path)
    db.resetdb()
