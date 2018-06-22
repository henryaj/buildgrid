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

import asyncio
import click
import grpc
import logging
import random

from buildgrid.bot import bot
from buildgrid._exceptions import BotError

from ..cli import pass_context

@click.group(short_help = 'Simple bot client')
@click.option('--port', default='50051')
@pass_context
def cli(context, port):
    context.logger = logging.getLogger(__name__)
    context.logger.info("Starting on port {}".format(port))

    context.channel = grpc.insecure_channel('localhost:{}'.format(port))
    context.port = port

@cli.command('create', short_help='Create a bot session')
@click.argument('parent', default='bgd_test')
@click.option('--continuous', is_flag=True)
@pass_context
def create(context, parent, continuous):
    """
    Simple dummy client. Creates a session, accepts leases, does work and
    updates the server. Can run this in continious mode.
    """

    context.logger.info("Creating a bot session")        

    try:
        bot_ = bot.Bot(_work, context.channel, parent, number_of_leases = 3)

    except KeyboardInterrupt:
        pass

    except Exception as e:
        context.logger.error(e)
        return

async def _work(lease):
    await asyncio.sleep(random.randint(1,5))
    return lease
