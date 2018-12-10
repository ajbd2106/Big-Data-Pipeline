#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Modelled after https://github.com/kennethreitz/samplemod

# Package meta-data.
NAME = 'FromDataSourceToKafka'
VERSION = 1.0
DESCRIPTION = 'Transfer data from a data source such as a file to a Kafka topic.'
URL = 'http://git.inceptum.local/BigData/FromDataSourceToKafka'
EMAIL = 'jaksicmislav@gmail.com'
AUTHOR = 'Mislav Jaksic'
REQUIRES_PYTHON = '>=3.0.0'


# What packages are required for this module to be executed?
REQUIRED = ["kafka-python"]

# What packages are optional?
EXTRAS = {}

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    url=URL,
    author_email=EMAIL,
    author=AUTHOR,
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
)

