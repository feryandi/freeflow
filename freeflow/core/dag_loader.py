#!/usr/bin/python
# -*- coding: utf-8 -*-
#
import pkgutil

from airflow import DAG
from airflow.models import BaseOperator


def explore_package(module_name):
    """
    The function will explore the package / module given and
    list all of the *.py and *.pyc files as child modules. The
    function is helpful to do dynamic importing based on whats
    inside a folder.

    :param module_name: target folder to be explored as module
    :type module_name: str
    :return: python files given as modules
    :rtype: Iterable(str)

    :raise AssertionError: when there is nothing inside the
                           folder
    """
    loader = pkgutil.get_loader(module_name)
    assert loader is not None, "Cannot find any files on module '{}'" \
                               .format(module_name)

    try:
        filename = loader.filename
    except Exception:
        filename = '/'.join(loader.get_filename().split('/')[:-1])

    for sub_module in pkgutil.walk_packages([filename]):
        _, sub_module_name, _ = sub_module
        qname = module_name + "." + sub_module_name
        yield qname
        explore_package(qname)


def get_instance_class(dag):
    """
    Retruns DAG instances and Task instances within a given
    DAG module.

    :param dag: DAG module
    :type dag: module
    :return: dictionary of dags and tasks instance
    :rtype: dict
    """
    dags = []
    tasks = []
    commands = dir(dag)
    for command in commands:
        if not command.startswith('__'):
            function = getattr(dag, command)
            if isinstance(function, DAG):
                dags.append(function)
            if isinstance(function, BaseOperator):
                tasks.append(function)
    return {'dags': dags, 'tasks': tasks}


def get_dag_files():
    """
    Returns DAG instances and its metadata that are exists
    on the designated dags folder. It also will ignore DAG
    in folder named udf (abbr. for user-defined functions)

    :return: list of DAG instance, its filename and
             DAG module
    :rtype: list(dict)
    """
    dag_files = []

    # TODO: Make sure that dags folder is always this one
    packages = explore_package('dags')
    for package in packages:
        # TODO: It should ignore all folder, not only udf
        if not package.startswith('dags.udf'):
            module = __import__(package)
            filename = package.split('.')[1]

            dag = getattr(module, filename)
            instance = get_instance_class(dag)
            dag_files.append({'filename': filename,
                              'dag': dag,
                              'instance': instance})
    return dag_files
