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
Execute command
=================

Request work to be executed and monitor status of jobs.
"""

import logging
import os
import sys
from urllib.parse import urlparse

import click
import grpc

from buildgrid.client.cas import upload
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import merkle_tree_maker

from ..cli import pass_context


@click.group(name='cas', short_help="Interact with the CAS server.")
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
    url = urlparse(remote)

    context.remote = '{}:{}'.format(url.hostname, url.port or 50051)
    context.instance_name = instance_name

    if url.scheme == 'http':
        context.channel = grpc.insecure_channel(context.remote)
    else:
        credentials = context.load_client_credentials(client_key, client_cert, server_cert)
        if not credentials:
            click.echo("ERROR: no TLS keys were specified and no defaults could be found.", err=True)
            sys.exit(-1)

        context.channel = grpc.secure_channel(context.remote, credentials)

    context.logger = logging.getLogger(__name__)
    context.logger.debug("Starting for remote {}".format(context.remote))


@cli.command('upload-dummy', short_help="Upload a dummy action. Should be used with `execute dummy-request`")
@pass_context
def upload_dummy(context):
    action = remote_execution_pb2.Action(do_not_cache=True)
    with upload(context.channel, instance=context.instance_name) as uploader:
        action_digest = uploader.put_message(action)

    if action_digest.ByteSize():
        click.echo('Success: Pushed digest "{}/{}"'
                   .format(action_digest.hash, action_digest.size_bytes))
    else:
        click.echo("Error: Failed pushing empty message.", err=True)


@cli.command('upload-files', short_help="Upload files to the CAS server.")
@click.argument('files', nargs=-1, type=click.Path(exists=True, dir_okay=False), required=True)
@pass_context
def upload_files(context, files):
    sent_digests, files_map = [], {}
    with upload(context.channel, instance=context.instance_name) as uploader:
        for file_path in files:
            context.logger.debug("Queueing {}".format(file_path))

            file_digest = uploader.upload_file(file_path, queue=True)

            files_map[file_digest.hash] = file_path
            sent_digests.append(file_digest)

    for file_digest in sent_digests:
        file_path = files_map[file_digest.hash]
        if os.path.isabs(file_path):
            file_path = os.path.relpath(file_path)
        if file_digest.ByteSize():
            click.echo('Success: Pushed "{}" with digest "{}/{}"'
                       .format(file_path, file_digest.hash, file_digest.size_bytes))
        else:
            click.echo('Error: Failed to push "{}"'.format(file_path), err=True)


@cli.command('upload-dir', short_help="Upload a directory to the CAS server.")
@click.argument('directory', nargs=1, type=click.Path(exists=True, file_okay=False), required=True)
@pass_context
def upload_dir(context, directory):
    sent_digests, nodes_map = [], {}
    with upload(context.channel, instance=context.instance_name) as uploader:
        for node, blob, path in merkle_tree_maker(directory):
            context.logger.debug("Queueing {}".format(path))

            node_digest = uploader.put_blob(blob, digest=node.digest, queue=True)

            nodes_map[node.digest.hash] = path
            sent_digests.append(node_digest)

    for node_digest in sent_digests:
        node_path = nodes_map[node_digest.hash]
        if os.path.isabs(node_path):
            node_path = os.path.relpath(node_path, start=directory)
        if node_digest.ByteSize():
            click.echo('Success: Pushed "{}" with digest "{}/{}"'
                       .format(node_path, node_digest.hash, node_digest.size_bytes))
        else:
            click.echo('Error: Failed to push "{}"'.format(node_path), err=True)
