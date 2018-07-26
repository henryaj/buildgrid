#!/usr/bin/env python3
#
# Copyright (C) 2018 Codethink Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  <http://www.apache.org/licenses/LICENSE-2.0>
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Authors:
#        Finn Ball <finn.ball@codethink.co.uk>

import setuptools
import sys

from _version import __version__

if sys.version_info[0] != 3 or sys.version_info[1] < 5:
    print("BuildGrid requires Python >= 3.5")
    sys.exit(1)

try:
    from setuptools import setup
except ImportError:
    print("BuildGrid requires setuptools in order to build. Install it using"
          " your package manager (usually python3-setuptools) or via pip (pip3"
          " install setuptools).")
    sys.exit(1)

setup(
    name="BuildGrid",
    version=__version__,
    license="Apache License, Version 2.0",
    description="A remote execution service",
    packages=setuptools.find_packages(),
    install_requires=[
        'setuptools',
        'protobuf',
        'grpcio',
        'Click',
        'boto3',
        'botocore',
    ],
    entry_points='''
    [console_scripts]
    bgd=app:cli
    ''',
    setup_requires=['pytest-runner'],
    tests_require=['pep8',
                   'boto3',
                   'botocore',
                   'moto',
                   'coverage == 4.4.0',
                   'pytest-cov >= 2.5.0',
                   'pytest-pep8',
                   'pytest-pylint',
                   'pytest >= 3.1.0',
                   'pylint >= 1.8 , < 2'],
)
