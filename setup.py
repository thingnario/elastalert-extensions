# -*- coding: utf-8 -*-
import os
from setuptools import setup


base_dir = os.path.dirname(__file__)
setup(
    name='elastalert-extensions',
    version='0.1.0',
    description='Customized Elastalert rule typs and alerts',
    author='Chun-da Chen',
    author_email='capitalm@thingnario.com',
    setup_requires='setuptools',
    license='Copyright 2017 thingnario LTD.',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    entry_points={},
    packages=['elastalert_extensions'],
    package_data={},
    install_requires=[
        'elastalert>=0.1.17',
        'pytz>=2017.2',
        'python-dateutil==2.6.1',
    ]
)
