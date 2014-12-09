#!/usr/bin/env python
# -*- encodig: utf-8 -*-

import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='blackbird-redis',
    version='0.1.2',
    description=(
        'Get monitorring stats of redis for blackbird '
    ),
    long_description=read('PROJECT.txt'),
    classifiers=[
      'Development Status :: 4 - Beta',
      'Programming Language :: Python :: 2',
      'Programming Language :: Python :: 2.6',
    ],
    author='makocchi',
    author_email='makocchi@gmail.com',
    url='https://github.com/Vagrants/blackbird-redis',
    data_files=[
        ('/opt/blackbird/plugins', ['redis.py']),
        ('/etc/blackbird/conf.d', ['redis.cfg'])
    ],
)

