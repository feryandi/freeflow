from setuptools import find_packages, setup

setup(name='freeflow',
      version='0.0',
      description='Airflow development and deployment, simplified.',
      url='http://github.com/feryandi/freeflow',
      author='Feryandi Nurdiantoro',
      author_email='feryandi.n@gmail.com',
      license='Apache License 2.0',
      packages=find_packages(),      
      install_requires=[
        'apache-airflow[crypto]==1.9.0',
        'flake8>=3.6.0',
        'google-api-python-client==1.7.4',
        'google-cloud-bigquery==1.6.0',
        'pendulum==2.0.3',
        'pandas-gbq==0.6.1',
        'oauth2client==4.1.3'
      ],
      zip_safe=False)