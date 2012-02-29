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
      license="BSD",
      author="Eugene Wu",
      author_email="eugenewu@mit.edu",
      url="http://github.com/sirrice/dbtruck",
      packages = find_packages(),
      scripts = ['bin/importmydata.py'],
      #package_data = { '' : ['*sentiment.pkl.gz'] },
      install_requires = ['argparse', 'DateUtils',],
      keywords= "library query db import")
