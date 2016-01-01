#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages

setup(name="dbtruck",
      version="0.0.8",
      description="Dump your data into your database",
      license="MIT",
      author="Eugene Wu",
      author_email="eugenewu@mit.edu",
      url="http://github.com/sirrice/dbtruck",
      include_package_data = True,      
      packages = find_packages(),
      package_dir = {'dbtruck' : 'dbtruck'},
      scripts = ['bin/importmydata.py'],
      package_data = { 'dbtruck' : ['data/*'] },
      install_requires = [
        'xlrd', 'openpyxl', 'click', 'DateUtils', 
			  'geopy',  'requests', "sqlalchemy", "psycopg2"
      ],
      keywords= "library query db import")
