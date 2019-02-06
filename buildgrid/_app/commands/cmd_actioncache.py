# Copyright (C) 2019 Bloomberg LP
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


import os
import sys
from textwrap import indent

import click
from google.protobuf import json_format

from buildgrid.client.actioncache import query
from buildgrid.client.authentication import setup_channel
from buildgrid.client.cas import download
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import create_digest, parse_digest

from ..cli import pass_context


@click.group(name='action-cache', short_help="Query and update the action cache service.")
@click.option('--remote', type=click.STRING, default='http://localhost:50051', show_default=True,
              help="Remote execution server's URL (port defaults to 50051 if no specified).")
@click.option('--auth-token', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Authorization token for the remote.")
@click.option('--client-key', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Private client key for TLS (PEM-encoded).")
@click.option('--client-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public client certificate for TLS (PEM-encoded).")
@click.option('--server-cert', type=click.Path(exists=True, dir_okay=False), default=None,
              help="Public server certificate for TLS (PEM-encoded)")
@click.option('--instance-name', type=click.STRING, default=None, show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert, server_cert):
    """Entry-point for the ``bgd action-cache`` CLI command group."""
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key, client_cert=client_cert,
                                           server_cert=server_cert)

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.instance_name = instance_name


@cli.command('get', short_help="Retrieves a cached action-result.")
@click.argument('action-digest-string', nargs=1, type=click.STRING, required=True)
@click.option('--json', is_flag=True, show_default=True,
              help="Print action result in JSON format.")
@pass_context
def get(context, action_digest_string, json):
    """Entry-point of the ``bgd action-cache get`` CLI command.

    Note:
        Digest strings are expected to be like: ``{hash}/{size_bytes}``.
    """
    action_digest = parse_digest(action_digest_string)
    if action_digest is None:
        click.echo("Error: Invalid digest string '{}'.".format(action_digest_string), err=True)
        sys.exit(-1)

    # Simply hit the action cache with the given action digest:
    with query(context.channel, instance=context.instance_name) as action_cache:
        action_result = action_cache.get(action_digest)

    if action_result is not None:
        if not json:
            action_result_digest = create_digest(action_result.SerializeToString())

            click.echo("Hit: {}/{}: Result cached with digest=[{}/{}]"
                       .format(action_digest.hash[:8], action_digest.size_bytes,
                               action_result_digest.hash, action_result_digest.size_bytes))

            # TODO: Print ActionResult details?

        else:
            click.echo(json_format.MessageToJson(action_result))

    else:
        click.echo("Miss: {}/{}: No associated result found in cache..."
                   .format(action_digest.hash[:8], action_digest.size_bytes))


@cli.command('update', short_help="Maps an action to a given action-result.")
@click.argument('action-digest-string', nargs=1, type=click.STRING, required=True)
@click.argument('action-result-digest-string', nargs=1, type=click.STRING, required=True)
@pass_context
def update(context, action_digest_string, action_result_digest_string):
    """Entry-point of the ``bgd action-cache update`` CLI command.

    Note:
        Digest strings are expected to be like: ``{hash}/{size_bytes}``.
    """
    action_digest = parse_digest(action_digest_string)
    if action_digest is None:
        click.echo("Error: Invalid digest string '{}'.".format(action_digest_string), err=True)
        sys.exit(-1)

    action_result_digest = parse_digest(action_result_digest_string)
    if action_result_digest is None:
        click.echo("Error: Invalid digest string '{}'.".format(action_result_digest_string), err=True)
        sys.exit(-1)

    # We have to download the ActionResult message fom CAS first...
    with download(context.channel, instance=context.instance_name) as downloader:
        action_result = downloader.get_message(action_result_digest,
                                               remote_execution_pb2.ActionResult())

        # And only then we can update the action cache for the given digest:
        with query(context.channel, instance=context.instance_name) as action_cache:
            action_result = action_cache.update(action_digest, action_result)

            if action_result is None:
                click.echo("Error: Failed updating cache result for action=[{}/{}]."
                           .format(action_digest.hash, action_digest.size_bytes), err=True)
                sys.exit(-1)
