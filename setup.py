# !/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='dsMate',
    version='0.2.0',
    packages=find_packages(),
    install_requires=['boto3', 'botocore', 'pandas', 's3fs', 'psycopg2-binary'],
    keywords='versioncontrol machinelearning datascience ds ml',
    url='https://github.com/emcasa/dsmate',
    classifiers=['Development Status :: 3 - Alpha', 'Programming Language :: Python :: 3.6'],
    author='Andre Sionek',
    author_email='andre.sionek@emcasa.com',
    description='Tool to keep version control of data science framework'
)
