#!/usr/bin/env python
from setuptools import setup, find_packages
except ImportError:
    import ez_setup
    ez_setup.use_setuptools()
from setuptools import setup, find_packages
import dbtruck

setup(name="dbtruck",
      version=tweeql.__version__,
      description="Dump your data into your database",
      license="BSD",
      author="Eugene Wu",
      author_email="eugenewu@mit.edu",
      url="http://github.com/sirrice/dbtruck",
      packages = find_packages(),
      scripts = ['dbtruck/dbtruck.py'],
      #package_data = { '' : ['*sentiment.pkl.gz'] },
      install_requires = ['argparse', 'dateutil',],
      keywords= "library query db import",
      zip_safe = True)
