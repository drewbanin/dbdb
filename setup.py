#!/usr/bin/env python
import os
import sys

from setuptools import setup

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print('Error: This package requires setuptools v40.1.0 or higher.')
    print('Please upgrade setuptools with "pip install --upgrade setuptools" '
          'and try again')
    sys.exit(1)

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbdb"
package_version = "0.0.1"
description = "The Drew Banin database (dbdb)"


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author="Drew Banin",
    author_email="drew@dbtlabs.com",
    url="https://github.com/drewbanin/dbdb",
    packages=find_namespace_packages(include='dbdb'),
    entry_points={
        'console_scripts': [
            'dbdb-cli=dbdb:cli',
            'dbdb-server=dbdb:server'
        ]
    },
    install_requires=[
        # TODO
        'hexdump==3.3',
        'networkx==2.8',
        'pyparsing==3.0.8',
        'tabulate==0.8.9',
        'Pympler==1.0.1',

    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.6.2",
)

