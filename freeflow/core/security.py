#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import subprocess


def encrypt():
    """
    Runnning encryption program designated and standardized for
    the Airflow project that using this library.

    This function currently not being implemented as developer
    should use Mozilla's **sops** directly to do encryption.
    """
    print("Use sops to encrypt the file.")
    print("Learn more at https://github.com/mozilla/sops")


def decrypt(path):
    """
    Running decryption program designated and standardized for
    the Airflow project that using this library. The function will
    run the **sops** program directly from the runner computer.
    Thus, it needs to be installed before hand.

    :param path: file path to be decrypted
    :type path: str
    :return: decrypted file content
    :rtype: str

    :raise RuntimeError: when the program's exit code is not zero
    :raise OSError: when the script couldn't run nor find the sops
    """
    command = ['sops', '-d', path]
    try:
        output = subprocess.check_output(command,
                                         stderr=subprocess.STDOUT)

    except subprocess.CalledProcessError as exception:
        raise RuntimeError(exception.output)

    except OSError as exception:
        print("Couldn't find sops. Are you sure it's installed?")
        print("Learn more at https://github.com/mozilla/sops")
        raise exception

    else:
        return output
