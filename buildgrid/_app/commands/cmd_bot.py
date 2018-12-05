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
Bot command
=================

Create a bot interface and request work
"""

from pathlib import Path, PurePath
import sys
from urllib.parse import urlparse

import click
import grpc

from buildgrid.bot import bot, interface, session
from buildgrid.bot.hardware.interface import HardwareInterface
from buildgrid.bot.hardware.device import Device
from buildgrid.bot.hardware.worker import Worker


from ..bots import buildbox, dummy, host
from ..cli import pass_context, setup_logging


@click.group(name='bot', short_help="Create and register bot clients.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if not specified).")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded)")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded)")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded)")
@click.option('--remote-cas', type=click.STRING, default=None, show_default=True,
              help="Remote CAS server's URL (port defaults to 11001 if not specified).")
@click.option('--cas-client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private CAS client key for TLS (PEM-encoded)")
@click.option('--cas-client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS client certificate for TLS (PEM-encoded)")
@click.option('--cas-server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS server certificate for TLS (PEM-encoded)")
@click.option('--update-period', type=click.FLOAT, default=0.5, show_default=True,
              help="Time period for bot updates to the server in seconds.")
@click.option('--parent', type=click.STRING, default='main', show_default=True,
              help="Targeted farm resource.")
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@pass_context
def cli(context, parent, update_period, remote, client_key, client_cert, server_cert,
        remote_cas, cas_client_key, cas_client_cert, cas_server_cert, verbose):
    setup_logging(verbosity=verbose)
    # Setup the remote execution server channel:
    url = urlparse(remote)

    context.remote = '{}:{}'.format(url.hostname, url.port or 50051)
    context.remote_url = remote
    context.update_period = update_period
    context.parent = parent

    if url.scheme == 'http':
        context.channel = grpc.insecure_channel(context.remote)

        context.client_key = None
        context.client_cert = None
        context.server_cert = None
    else:
        credentials = context.load_client_credentials(client_key, client_cert, server_cert)
        if not credentials:
            click.echo("ERROR: no TLS keys were specified and no defaults could be found.", err=True)
            sys.exit(-1)

        context.channel = grpc.secure_channel(context.remote, credentials)

        context.client_key = credentials.client_key
        context.client_cert = credentials.client_cert
        context.server_cert = credentials.server_cert

    # Setup the remote CAS server channel, if separated:
    if remote_cas is not None and remote_cas != remote:
        cas_url = urlparse(remote_cas)

        context.remote_cas = '{}:{}'.format(cas_url.hostname, cas_url.port or 11001)
        context.remote_cas_url = remote_cas

        if cas_url.scheme == 'http':
            context.cas_channel = grpc.insecure_channel(context.remote_cas)

            context.cas_client_key = None
            context.cas_client_cert = None
            context.cas_server_cert = None
        else:
            cas_credentials = context.load_client_credentials(cas_client_key, cas_client_cert, cas_server_cert)
            if not cas_credentials:
                click.echo("ERROR: no TLS keys were specified and no defaults could be found.", err=True)
                sys.exit(-1)

            context.cas_channel = grpc.secure_channel(context.remote_cas, cas_credentials)

            context.cas_client_key = cas_credentials.client_key
            context.cas_client_cert = cas_credentials.client_cert
            context.cas_server_cert = cas_credentials.server_cert

    else:
        context.remote_cas = context.remote
        context.remote_cas_url = remote

        context.cas_channel = context.channel

        context.cas_client_key = context.client_key
        context.cas_client_cert = context.client_cert
        context.cas_server_cert = context.server_cert

    bot_interface = interface.BotInterface(context.channel)

    worker = Worker()
    worker.add_device(Device())
    hardware_interface = HardwareInterface(worker)

    context.bot_interface = bot_interface
    context.hardware_interface = hardware_interface


@cli.command('dummy', short_help="Run a dummy session simply returning leases.")
@pass_context
def run_dummy(context):
    """
    Creates a session, accepts leases, does fake work and updates the server.
    """
    try:
        bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                         dummy.work_dummy, context, context.update_period)
        b = bot.Bot(bot_session)
        b.session()
    except KeyboardInterrupt:
        pass


@cli.command('host-tools', short_help="Runs commands using the host's tools.")
@pass_context
def run_host_tools(context):
    """
    Downloads inputs from CAS, runs build commands using host-tools and uploads
    result back to CAS.
    """
    try:
        bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                         host.work_host_tools, context, context.update_period)
        b = bot.Bot(bot_session)
        b.session()
    except KeyboardInterrupt:
        pass


@cli.command('buildbox', short_help="Run commands using the BuildBox tool.")
@click.option('--fuse-dir', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'fuse')),
              help="Main mount-point location.")
@click.option('--local-cas', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'cas')),
              help="Local CAS cache directory.")
@pass_context
def run_buildbox(context, local_cas, fuse_dir):
    """
    Uses BuildBox to run build commands.
    """
    context.local_cas = local_cas
    context.fuse_dir = fuse_dir

    try:
        bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                         buildbox.work_buildbox, context, context.update_period)
        b = bot.Bot(bot_session)
        b.session()
    except KeyboardInterrupt:
        pass
