#!/usr/bin/env python3
#
# Copyright (C) 2018 Bloomberg LP
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


import os
import re
import sys

from buildgrid._version import __version__

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


# Load main requirements from file:
with open('requirements.txt') as requirements_file:
    install_requirements = requirements_file.read().splitlines()

auth_requirements = []
# Load 'auth' requirements from dedicated file:
if os.path.isfile('requirements.auth.txt'):
    with open('requirements.auth.txt') as requirements_file:
        auth_requirements = requirements_file.read().splitlines()

docs_requirements = []
# Load 'docs' requirements from dedicated file:
if os.path.isfile('requirements.docs.txt'):
    with open('requirements.docs.txt') as requirements_file:
        docs_requirements = requirements_file.read().splitlines()

tests_requirements = []
# Load 'tests' requirements from dedicated file:
if os.path.isfile('requirements.tests.txt'):
    with open('requirements.tests.txt') as requirements_file:
        tests_requirements = requirements_file.read().splitlines()

db_requirements = []
# Load 'db' requirements from dedicated file:
if os.path.isfile('requirements.db.txt'):
    with open('requirements.db.txt') as requirements_file:
        db_requirements = requirements_file.read().splitlines()

redis_requirements = []
# Load 'redis' requirements from dedicated file:
if os.path.isfile('requirements.redis.txt'):
    with open('requirements.redis.txt') as requirements_file:
        redis_requirements = requirements_file.read().splitlines()

setup(
    name="BuildGrid",
    version=__version__,
    license="Apache License, Version 2.0",
    description="A remote execution service",
    cmdclass={
        'build_grpc': BuildGRPC, },
    packages=find_packages(),
    package_data={'buildgrid.server.persistence.sql': ['alembic/*', 'alembic/**/*']},
    python_requires='>= 3.5.3',  # janus requirement
    install_requires=install_requirements,
    setup_requires=['pytest-runner'],
    tests_require=tests_requirements,
    extras_require={
        'auth': auth_requirements,
        'database': db_requirements,
        'redis': redis_requirements,
        'docs': docs_requirements,
        'tests': tests_requirements, },
    entry_points={
        'console_scripts': [
            'bgd = buildgrid._app:cli',
        ]
    }
)
