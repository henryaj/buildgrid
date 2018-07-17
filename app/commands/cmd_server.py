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
Server command
=================

Create a BuildGrid server.
"""


import asyncio
import click
import logging

from buildgrid.server import build_grid_server
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache

from ..cli import pass_context

@click.group(short_help = "Start local server")
@pass_context
def cli(context):
    context.logger = logging.getLogger(__name__)
    context.logger.info("BuildGrid server booting up")

@cli.command('start', short_help='Starts server')
@click.option('--port', default='50051')
@pass_context
def start(context, port):
    context.logger.info("Starting on port {}".format(port))

    loop = asyncio.get_event_loop()

    # TODO Allow user to configure this with command-line arguments
    cas_storage = LRUMemoryCache(512 * 1024 * 1024)
    server = build_grid_server.BuildGridServer(port, cas_storage=cas_storage)
    try:
        asyncio.ensure_future(server.start())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()

@cli.command('stop', short_help='Stops server')
@pass_context
def stop(context):
    context.logger.error("Not implemented yet")
