import glob
import subprocess
import os

def encrypt():
  print("Use sops to encrypt the file.")
  print("Learn more at https://github.com/mozilla/sops")

def decrypt(path):
  cmd = ['sops', '-d', path]
  try:
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError as exception:
    raise RuntimeError(exception.output)
  except OSError as exception:
    print("Couldn't find sops. Are you sure it's installed?")
    print("Learn more at https://github.com/mozilla/sops")
    raise exception
  else:
    return output
