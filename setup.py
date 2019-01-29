from setuptools import find_packages, setup

setup(name='freeflow',
      version='0.1.6',
      description='Airflow development and deployment, simplified.',
      url='http://github.com/feryandi/freeflow',
      author='Feryandi Nurdiantoro',
      author_email='feryandi.n@gmail.com',
      license='Apache License 2.0',
      scripts=['freeflow/bin/freeflow'],
      packages=find_packages(),
      include_package_data=True,
      install_requires=[
        'apache-airflow[crypto]==1.9.0',
        'flake8>=3.6.0',
        'google-api-python-client==1.7.4',
        'google-cloud-bigquery==1.8.1',
        'pendulum==2.0.4',
        'pandas-gbq==0.6.1',
        'pandas==0.23.4',
        'pytest==4.1.1',
        'oauth2client==4.1.3',
        'google-cloud-storage==1.13.2'
      ],
      zip_safe=False)
