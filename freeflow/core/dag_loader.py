#!/usr/bin/python
# -*- coding: utf-8 -*-
import pkgutil
import os

from airflow import DAG
from airflow.models import BaseOperator


def explore_package(module_name):
    """ This function returns all of the DAG within dag folder """
    loader = pkgutil.get_loader(module_name)
    assert loader is not None, "Cannot find any DAG files on module '{}'" \
                               .format(module_name)

    for sub_module in pkgutil.walk_packages([loader.filename]):
        _, sub_module_name, _ = sub_module
        qname = module_name + "." + sub_module_name
        yield qname
        explore_package(qname)


def get_instance_class(dag):
    """
    This functions will return dag instance
    and task instance of the DAG file
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
    dag_files = []
    root_dir = os.environ.get('AIRFLOW_HOME', '')

    packages = explore_package('dags'.format(root_dir))  # TO-DO
    for package in packages:
        if not package.startswith('dags.udf'):  # TO-DO
            module = __import__(package)
            filename = package.split('.')[1]

            dag = getattr(module, filename)
            instance = get_instance_class(dag)
            dag_files.append({'filename': filename,
                              'dag': dag,
                              'instance': instance})
    return dag_files

# get_dag_files()


# @pytest.fixture(autouse=True, scope='session')
# def airflow_test_mode():
#     airflow.configuration.load_test_config()
#     airflow.utils.db.initdb()
