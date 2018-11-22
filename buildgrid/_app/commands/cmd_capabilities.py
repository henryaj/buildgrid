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


import sys
from urllib.parse import urlparse

import click
import grpc

from buildgrid.client.capabilities import CapabilitiesInterface

from ..cli import pass_context


@click.command(name='capabilities', short_help="Capabilities service.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded)")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded)")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded)")
@click.option('--instance-name', type=click.STRING, default='main', show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, client_key, client_cert, server_cert):
    click.echo("Getting capabilities...")
    url = urlparse(remote)

    remote = '{}:{}'.format(url.hostname, url.port or 50051)
    instance_name = instance_name

    if url.scheme == 'http':
        channel = grpc.insecure_channel(remote)
    else:
        credentials = context.load_client_credentials(client_key, client_cert, server_cert)
        if not credentials:
            click.echo("ERROR: no TLS keys were specified and no defaults could be found.", err=True)
            sys.exit(-1)

        channel = grpc.secure_channel(remote, credentials)

    interface = CapabilitiesInterface(channel)
    response = interface.get_capabilities(instance_name)
    click.echo(response)
