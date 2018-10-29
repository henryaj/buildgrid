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

import os
import re
import sys

from _version import __version__

if sys.version_info[0] != 3 or sys.version_info[1] < 5:
    print("BuildGrid requires Python >= 3.5")
    sys.exit(1)

try:
    from setuptools import setup, find_packages, Command
except ImportError:
    print("BuildGrid requires setuptools in order to build. Install it using"
          " your package manager (usually python3-setuptools) or via pip (pip3"
          " install setuptools).")
    sys.exit(1)


class BuildGRPC(Command):
    """Command to generate project *_pb2.py modules from proto files."""

    description = 'build gRPC protobuf modules'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            import grpc_tools.command
        except ImportError:
            print("BuildGrid requires grpc_tools in order to build gRPC modules.\n"
                  "Install it via pip (pip3 install grpcio-tools).")
            exit(1)

        protos_root = 'buildgrid/_protos'

        grpc_tools.command.build_package_protos(protos_root)

        # Postprocess imports in generated code
        for root, _, files in os.walk(protos_root):
            for filename in files:
                if filename.endswith('.py'):
                    path = os.path.join(root, filename)
                    with open(path, 'r') as f:
                        code = f.read()

                    # All protos are in buildgrid._protos
                    code = re.sub(r'^from ', r'from buildgrid._protos.',
                                  code, flags=re.MULTILINE)
                    # Except for the core google.protobuf protos
                    code = re.sub(r'^from buildgrid._protos.google.protobuf', r'from google.protobuf',
                                  code, flags=re.MULTILINE)

                    with open(path, 'w') as f:
                        f.write(code)


def get_cmdclass():
    cmdclass = {
        'build_grpc': BuildGRPC,
    }
    return cmdclass

tests_require = [
    'coverage >= 4.5.0',
    'moto',
    'pep8',
    'psutil',
    'pytest >= 3.8.0',
    'pytest-cov >= 2.6.0',
    'pytest-pep8',
    'pytest-pylint',
]

docs_require = [
    # rtd-theme broken in Sphinx >= 1.8, this breaks search functionality.
    'sphinx == 1.7.8',
    'sphinx-click',
    'sphinx-rtd-theme',
    'sphinxcontrib-apidoc',
    'sphinxcontrib-napoleon',
]

setup(
    name="BuildGrid",
    version=__version__,
    cmdclass=get_cmdclass(),
    license="Apache License, Version 2.0",
    description="A remote execution service",
    packages=find_packages(),
    install_requires=[
        'protobuf',
        'grpcio',
        'Click',
        'pyaml',
        'boto3 < 1.8.0',
        'botocore < 1.11.0',
    ],
    entry_points={
        'console_scripts': [
            'bgd = buildgrid._app:cli',
        ]
    },
    setup_requires=['pytest-runner'],
    tests_require=tests_require,
    extras_require={
        'docs': docs_require,
        'tests': tests_require,
    },
)
