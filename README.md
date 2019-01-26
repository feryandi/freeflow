# Freeflow
Airflow development and deployment, hopefully simplified.

## What is this?
Ever think of starting another Airflow project and you are keep doing the same thing over-and-over again? I did.
Starting a project for me have always been putting all together the development and deployment process in place so we could collaborate flawlessly and make sure no one could easilly f*cked up our production.

Those things are consists of standardization of folder structure, tracked Airflow's variables and connections, encrypt-decrypt secrets, pre-tests of DAGs, and deployment script.

## Installation
The package could be installed by running,

`pip install freeflow`

## How to start?
Quite easy, just create a new project with these script

`freeflow start your_awesome_project`

It'll generate a folder structure, examples, and base script for you to use.

## How to use it?
### Generated Template
The template generate the folder structure and some example files to get started.
The other thing is it also generate the `manage.py` script.

This file used to run CLI commands that will help your development and deployment.
You could run it by using normal python command: `python manage.py -h`

### Folder Structure
```
├── conf
├── dags
├─┬ data
┆ ├── conf
┆ ├── conn
┆ ├── pool
┆ └── vars
├── test 
└── manage.py
```

### Airflow Environments
You can make different environment to behave differently on terms of the variables, connections, pools, or deployment.

By adding a new file with `*.cfg` extension on `conf` folder, you successfully added a new environment type. By default, a `default.cfg` is generated from template as an example.
```
[imports]
conf =  # The name of Airflow configuration file to be imported
vars =  # The name of Airflow variables file to be imported
conn =  # The name of folder to be imported for connections
pool =  # The name of folder to be imported for pools

[composer] # This used when you want to deploy it to Google Composer
project_id = 
name = 
location = 
bucket = 
```
To use the environment, simply add `--env env_name` when using the `manage.py`.
Example: `python manage.py --env stg init`

### Changing Airflow Configuration
An example of this could be seen in the folder `data/conf/`. You just put the configuration that you want to override.
```
[core]
load_examples = False
```

### Adding Variables
Adding variables is simply putting a JSON file into the `data/vars/` folder and imports it via the environment congiuration (`conf/`). The JSON file is following the format that Airflow generated when you try to export the variables via the UI.

### Adding Connections
Adding connection is also by putting a JSON file containing all the connection details. A basic template for the JSON is,
```
{
  "conn_id": "example_connection", /* Required */
  "conn_type": "", /* Required */
  "conn_uri": "",
  "conn_host": "",
  "conn_login": "",
  "conn_password": "",
  "conn_port": "",
  "conn_schema": "",
  "conn_extra": {
    "extra__example": "value" /* Differs for each connection type available */
  }
}
```

#### Security Concern
Usually, connection credentials cannot be put on git (or, anywhere) as a plaintext. And you shouldn't, if you still do it.

This framework will help you with that by automatically using [mozilla/sops](https://github.com/mozilla/sops) for encryption and decryption.
To use this feature, encrypt your connection JSON file using `sops` and put it on the folder with `*.enc.*` in the filename.

Those file will automatically decrypted when you are trying to deploy the Airflow.
It will not create any kind of data that could expose the encrypted data.

### Adding Pools
Similar to adding connection, adding pools also using JSON with these template,
```
{
  "name": "example_pool",
  "slot_count": 10,
  "pool_description": "An example of pool"
}
```

### Adding DAGs
Adding DAGs is pretty much the same as normal Airflow. Just put it on the `dags/` folder.

### Running Airflow
#### Running Locally
To run locally, first you have to initialize the Airflow by using the command:

`python manage.py --env your_env init`

Then, deploy the Airflow by using:

`python manage.py --env your_env deploy --type direct`

Next, run the Airflow as usual by using `airflow webserver` and `airflow scheduler`.

#### Running on Composer
Running on Composer doesn't need initialization. Just make sure that you already put the needed Composer environment data on your environment configuration, and then run:

`python manage.py --env your_composer_env deploy --type composer`

Make sure that you are already authenticated using `gcloud auth application-default login` and that your account have the access to deploy the Composer.

### Running Tests
The tests are being divided into two, called: `general` and `dags`.

#### General Tests
General tests are tests that are provided by this template to make sure that your script is running smoothly without triggering the red ribbon flag on upper UI.

This tests includes:
- DAG checks: name, validity, tasks name.
- ExternalTaskSensor checks: make sure it doesn't stuck in a stupid way (e.g. wrong DAG name reffered)
- BigQueryOperator checks: dry run all the queries to make sure no invalid query getting pushed.
- ... hopefully more

#### DAGs Tests
This the tests that user created. This basically tests that resides on the `tests/` folder.

## Disclaimer
This is heavilly experimental and I do not guarantee anything. If you have any issues or feedbacks please put it on this repository issue page.

Have a great day!
