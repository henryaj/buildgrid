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

import click
from google.protobuf import json_format

from buildgrid.client.authentication import setup_channel
from buildgrid.client.capabilities import CapabilitiesInterface
from buildgrid._exceptions import InvalidArgumentError

from ..cli import pass_context


@click.command(name='capabilities', short_help="Capabilities service.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--instance-name', type=click.STRING, default=None, show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert, server_cert):
    """Entry point for the bgd-capabilities CLI command group."""
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key, client_cert=client_cert,
                                           server_cert=server_cert)

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.instance_name = instance_name

    interface = CapabilitiesInterface(context.channel)
    response = interface.get_capabilities(context.instance_name)

    click.echo(json_format.MessageToJson(response))
