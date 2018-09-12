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

import asyncio
import logging
import sys

import click

from buildgrid.server.controller import ExecutionController
from buildgrid.server.actioncache.storage import ActionCache
from buildgrid.server.cas.instance import ByteStreamInstance, ContentAddressableStorageInstance
from buildgrid.server.referencestorage.storage import ReferenceCache

from ..cli import pass_context
from ..settings import parser
from ..server import BuildGridServer


@click.group(name='server', short_help="Start a local server instance.")
@pass_context
def cli(context):
    context.logger = logging.getLogger(__name__)


@cli.command('start', short_help="Setup a new server instance.")
@click.argument('CONFIG', type=click.Path(file_okay=True, dir_okay=False, writable=False))
@pass_context
def start(context, config):
    with open(config) as f:
        settings = parser.get_parser().safe_load(f)

    server_settings = settings['server']
    insecure_mode = server_settings['insecure-mode']

    credentials = None
    if not insecure_mode:
        server_key = server_settings['tls-server-key']
        server_cert = server_settings['tls-server-cert']
        client_certs = server_settings['tls-client-certs']
        credentials = context.load_server_credentials(server_key, server_cert, client_certs)

        if not credentials:
            click.echo("ERROR: no TLS keys were specified and no defaults could be found.\n" +
                       "Set `insecure-mode: false` in order to deactivate TLS encryption.\n", err=True)
            sys.exit(-1)

    instances = settings['instances']

    execution_controllers = _instance_maker(instances, ExecutionController)

    execution_instances = {}
    bots_interfaces = {}
    operations_instances = {}

    # TODO: map properly in parser
    # Issue 82
    for k, v in execution_controllers.items():
        execution_instances[k] = v.execution_instance
        bots_interfaces[k] = v.bots_interface
        operations_instances[k] = v.operations_instance

    reference_caches = _instance_maker(instances, ReferenceCache)
    action_caches = _instance_maker(instances, ActionCache)
    cas = _instance_maker(instances, ContentAddressableStorageInstance)
    bytestreams = _instance_maker(instances, ByteStreamInstance)

    port = server_settings['port']
    server = BuildGridServer(port=port,
                             credentials=credentials,
                             execution_instances=execution_instances,
                             bots_interfaces=bots_interfaces,
                             operations_instances=operations_instances,
                             reference_storage_instances=reference_caches,
                             action_cache_instances=action_caches,
                             cas_instances=cas,
                             bytestream_instances=bytestreams)

    context.logger.info("Starting server on port {}".format(port))
    loop = asyncio.get_event_loop()
    try:
        server.start()
        loop.run_forever()

    except KeyboardInterrupt:
        pass

    finally:
        context.logger.info("Stopping server")
        server.stop()
        loop.close()


# Turn away now if you want to keep your eyes
def _instance_maker(instances, service_type):
    # TODO get this mapped in parser
    made = {}

    for instance in instances:
        services = instance['services']
        instance_name = instance['name']
        for service in services:
            if isinstance(service, service_type):
                made[instance_name] = service
    return made
