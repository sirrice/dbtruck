#!/usr/bin/env python
try:
    from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages
import dbtruck

setup(name="dbtruck",
      version=dbtruck.__version__,
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
      install_requires = ['xlrd', 'pyxl', 'argparse', 'DateUtils', 
			  'geopy', 'openpyxl', 'requests', 'pyquery', "sqlalchemy", "lxml", "psycopg2"],
      keywords= "library query db import")
