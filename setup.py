from setuptools import find_packages, setup

setup(name='freeflow',
      version='0.1.10',
      description='Airflow development and deployment, simplified.',
      url='http://github.com/feryandi/freeflow',
      author='Feryandi Nurdiantoro',
      author_email='feryandi.n@gmail.com',
      license='Apache License 2.0',
      scripts=['freeflow/bin/freeflow'],
      packages=find_packages(),
      include_package_data=True,
      install_requires=[
        'flake8>=3.6.0',
        'google-cloud-storage==1.13.2',
        'six==1.12.0',
        'pytest==4.1.1'
      ],
      zip_safe=False)
