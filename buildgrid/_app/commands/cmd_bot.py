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

import logging

from pathlib import Path, PurePath

import click
import grpc

from buildgrid.bot import bot, bot_interface
from buildgrid.bot.bot_session import BotSession, Device, Worker

from ..bots import buildbox, dummy, temp_directory
from ..cli import pass_context


@click.group(name='bot', short_help="Create and register bot clients.")
@click.option('--parent', type=click.STRING, default='bgd_test', show_default=True,
              help="Targeted farm resource.")
@click.option('--port', type=click.INT, default='50051', show_default=True,
              help="Remote server's port number.")
@click.option('--host', type=click.STRING, default='localhost', show_default=True,
              help="Renote server's hostname.")
@pass_context
def cli(context, host, port, parent):
    channel = grpc.insecure_channel('{}:{}'.format(host, port))
    interface = bot_interface.BotInterface(channel)

    context.logger = logging.getLogger(__name__)
    context.logger.info("Starting on port {}".format(port))
    context.channel = channel

    worker = Worker()
    worker.add_device(Device())

    bot_session = BotSession(parent, interface)
    bot_session.add_worker(worker)

    context.bot_session = bot_session


@cli.command('dummy', short_help="Run a dummy session simply returning leases.")
@pass_context
def run_dummy(context):
    """
    Simple dummy client. Creates a session, accepts leases, does fake work and
    updates the server.
    """
    try:
        b = bot.Bot(context.bot_session)
        b.session(dummy.work_dummy,
                  context)
    except KeyboardInterrupt:
        pass


@cli.command('temp-directory', short_help="Runs commands in temp directory and uploads results.")
@click.option('--instance-name', type=click.STRING, default='testing', show_default=True,
              help="Targeted farm instance name.")
@pass_context
def run_temp_directory(context, instance_name):
    """ Downloads files and command from CAS and runs
    in a temp directory, uploading result back to CAS
    """
    context.instance_name = instance_name
    try:
        b = bot.Bot(context.bot_session)
        b.session(temp_directory.work_temp_directory,
                  context)
    except KeyboardInterrupt:
        pass


@cli.command('buildbox', short_help="Run commands using the BuildBox tool.")
@click.option('--fuse-dir', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'fuse')),
              help="Main mount-point location.")
@click.option('--local-cas', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'cas')),
              help="Local CAS cache directory.")
@click.option('--client-cert', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'client.crt')),
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--client-key', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'client.key')),
              help="Private client key for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(readable=False), default=str(PurePath(Path.home(), 'server.crt')),
              help="Public server certificate for TLS (PEM-encoded).")
@click.option('--port', type=click.INT, default=11001, show_default=True,
              help="Remote CAS server port.")
@click.option('--remote', type=click.STRING, default='localhost', show_default=True,
              help="Remote CAS server hostname.")
@pass_context
def run_buildbox(context, remote, port, server_cert, client_key, client_cert, local_cas, fuse_dir):
    """
    Uses BuildBox to run commands.
    """

    context.logger.info("Creating a bot session")

    context.remote = remote
    context.port = port
    context.server_cert = server_cert
    context.client_key = client_key
    context.client_cert = client_cert
    context.local_cas = local_cas
    context.fuse_dir = fuse_dir

    try:
        b = bot.Bot(context.bot_session)
        b.session(work=buildbox.work_buildbox,
                  context=context)
    except KeyboardInterrupt:
        pass
