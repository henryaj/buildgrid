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


@click.group(short_help="Create a bot client")
@click.option('--parent', default='bgd_test')
@click.option('--port', default='50051')
@click.option('--host', default='localhost')
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


@cli.command('dummy', short_help='Create a dummy bot session which just returns lease')
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


@cli.command('temp-directory', short_help='Runs commands in temp directory and uploads results')
@click.option('--instance-name', default='testing')
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


@cli.command('buildbox', short_help="Create a bot session with busybox")
@click.option('--fuse-dir', show_default=True, default=str(PurePath(Path.home(), 'fuse')))
@click.option('--local-cas', show_default=True, default=str(PurePath(Path.home(), 'cas')))
@click.option('--client-cert', show_default=True, default=str(PurePath(Path.home(), 'client.crt')))
@click.option('--client-key', show_default=True, default=str(PurePath(Path.home(), 'client.key')))
@click.option('--server-cert', show_default=True, default=str(PurePath(Path.home(), 'server.crt')))
@click.option('--port', show_default=True, default=11001)
@click.option('--remote', show_default=True, default='localhost')
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
