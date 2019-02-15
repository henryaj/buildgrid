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


"""
Server command
=================

Create a BuildGrid server.
"""

import sys

import click

from buildgrid._exceptions import PermissionDeniedError
from buildgrid.server._authentication import AuthMetadataMethod, AuthMetadataAlgorithm
from buildgrid.server.instance import BuildGridServer
from buildgrid.server._monitoring import MonitoringOutputType, MonitoringOutputFormat
from buildgrid.utils import read_file

from ..cli import pass_context, setup_logging
from ..settings import parser


@click.group(name='server', short_help="Start a local server instance.")
@pass_context
def cli(context):
    pass


@cli.command('start', short_help="Setup a new server instance.")
@click.argument('CONFIG',
                type=click.Path(file_okay=True, dir_okay=False, exists=True, writable=False))
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@pass_context
def start(context, config, verbose):
    """Entry point for the bgd-server CLI command group."""
    setup_logging(verbosity=verbose)

    with open(config) as f:
        settings = parser.get_parser().safe_load(f)

    try:
        server = _create_server_from_config(settings)

    except KeyError as e:
        click.echo("ERROR: Could not parse config: {}.\n".format(str(e)), err=True)
        sys.exit(-1)

    try:
        server.start()

    except KeyboardInterrupt:
        pass

    finally:
        server.stop()


def _create_server_from_config(configuration):
    """Parses configuration and setup a fresh server instance."""
    kargs = {}

    try:
        network = configuration['server']
        instances = configuration['instances']

    except KeyError as e:
        click.echo("Error: Section missing from configuration: {}.".format(e), err=True)
        sys.exit(-1)

    if 'authorization' in configuration:
        authorization = configuration['authorization']

        try:
            if 'method' in authorization:
                kargs['auth_method'] = AuthMetadataMethod(authorization['method'])

            if 'secret' in authorization:
                kargs['auth_secret'] = read_file(authorization['secret']).decode().strip()

            if 'algorithm' in authorization:
                kargs['auth_algorithm'] = AuthMetadataAlgorithm(authorization['algorithm'])

        except (ValueError, OSError) as e:
            click.echo("Error: Configuration, {}.".format(e), err=True)
            sys.exit(-1)

    if 'monitoring' in configuration:
        monitoring = configuration['monitoring']

        try:
            if 'enabled' in monitoring:
                kargs['monitor'] = monitoring['enabled']

            if 'endpoint-type' in monitoring:
                kargs['mon_endpoint_type'] = MonitoringOutputType(monitoring['endpoint-type'])

            if 'endpoint-location' in monitoring:
                kargs['mon_endpoint_location'] = monitoring['endpoint-location']

            if 'serialization-format' in monitoring:
                kargs['mon_serialisation_format'] = MonitoringOutputFormat(monitoring['serialization-format'])

        except (ValueError, OSError) as e:
            click.echo("Error: Configuration, {}.".format(e), err=True)
            sys.exit(-1)

    if 'thread-pool-size' in configuration:
        try:
            kargs['max_workers'] = int(configuration['thread-pool-size'])

        except ValueError as e:
            click.echo("Error: Configuration, {}.".format(e), err=True)
            sys.exit(-1)

    server = BuildGridServer(**kargs)

    try:
        for channel in network:
            server.add_port(channel.address, channel.credentials)

    except PermissionDeniedError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    for instance in instances:
        instance_name = instance['name']
        services = instance['services']

        for service in services:
            service.register_instance_with_server(instance_name, server)

    return server
