#!/usr/bin/env python

# For normal libs
# from distutils.core import setup

# For egg files
from setuptools import setup

setup(name='prince',
      version='0.1',
      description='Hadoop/MapReduce API',
      author='Emmanuel Goossaert',
      author_email='emmanuel[at]goossaert[dot]com',
      url='http://wiki.github.com/goossaert/prince/',
      packages=['prince'],
      )

