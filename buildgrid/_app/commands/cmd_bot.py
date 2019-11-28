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

import click

from buildgrid.bot import bot, interface, session
from buildgrid.bot.hardware.interface import HardwareInterface
from buildgrid.bot.hardware.device import Device
from buildgrid.bot.hardware.worker import Worker
from buildgrid.client.channel import setup_channel
from buildgrid._exceptions import InvalidArgumentError

from ..bots import buildbox, dummy, host
from ..cli import pass_context, setup_logging


@click.group(name='bot', short_help="Create and register bot clients.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if not specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--remote-cas', type=click.STRING, default=None, show_default=True,
              help="Remote CAS server's URL (port defaults to 11001 if not specified).")
@click.option('--cas-client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private CAS client key for TLS (PEM-encoded).")
@click.option('--cas-client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS client certificate for TLS (PEM-encoded).")
@click.option('--cas-server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public CAS server certificate for TLS (PEM-encoded).")
@click.option('--update-period', type=click.FLOAT, default=30, show_default=True,
              help="Time period for bot updates to the server in seconds.")
@click.option('--executing-update-period', type=click.FLOAT, default=5, show_default=True,
              help="Time period for bot updates to the server whilst execution is happening, "
                   "in seconds.")
@click.option('--parent', type=click.STRING, default=None, show_default=True,
              help="Targeted farm resource.")
@click.option('-w', '--worker-property', nargs=2, type=(click.STRING, click.STRING), multiple=True,
              help="List of key-value pairs of worker properties.")
@click.option('-v', '--verbose', count=True,
              help='Increase log verbosity level.')
@pass_context
def cli(context, parent, update_period, executing_update_period, remote, auth_token,
        client_key, client_cert, server_cert, remote_cas, cas_client_key, cas_client_cert,
        cas_server_cert, worker_property, verbose):
    setup_logging(verbosity=verbose)
    # Setup the remote execution server channel:
    try:
        context.channel, details = setup_channel(remote, auth_token=auth_token,
                                                 server_cert=server_cert,
                                                 client_key=client_key,
                                                 client_cert=client_cert)

        if remote_cas is not None and remote_cas != remote:
            context.cas_channel, details = setup_channel(remote_cas,
                                                         server_cert=cas_server_cert,
                                                         client_key=cas_client_key,
                                                         client_cert=cas_client_cert)
            context.remote_cas_url = remote_cas

        else:
            context.cas_channel = context.channel
            context.remote_cas_url = remote

        context.cas_client_key, context.cas_client_cert, context.cas_server_cert = details

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.parent = parent

    bot_interface = interface.BotInterface(
        context.channel, update_period, executing_update_period)

    worker_properties_dict = {}
    for property_name, property_value in worker_property:
        if property_name not in worker_properties_dict:
            worker_properties_dict[property_name] = set()
        worker_properties_dict[property_name].add(property_value)

    worker = Worker()
    worker.add_device(Device(properties=worker_properties_dict))
    hardware_interface = HardwareInterface(worker)

    context.bot_interface = bot_interface
    context.hardware_interface = hardware_interface


@cli.command('dummy', short_help="Run a dummy session simply returning leases.")
@pass_context
def run_dummy(context):
    """
    Creates a session, accepts leases, does fake work and updates the server.
    """
    bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                     dummy.work_dummy, context)
    b = bot.Bot(bot_session)
    b.session()


@cli.command('host-tools', short_help="Runs commands using the host's tools.")
@pass_context
def run_host_tools(context):
    """
    Downloads inputs from CAS, runs build commands using host-tools and uploads
    result back to CAS.
    """
    from pycallgraph import PyCallGraph
    from pycallgraph.output import GraphvizOutput

    with PyCallGraph(output=GraphvizOutput()):
        bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                         host.work_host_tools, context)
        b = bot.Bot(bot_session)
        b.session()

    with open('/tmp/profile', 'w') as dotfile:
        dotfile.write(graphviz.generate())


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

    bot_session = session.BotSession(context.parent, context.bot_interface, context.hardware_interface,
                                     buildbox.work_buildbox, context)
    b = bot.Bot(bot_session)
    b.session()
