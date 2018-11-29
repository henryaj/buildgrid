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

import os
import sys

import click

from buildgrid.client.authentication import setup_channel
from buildgrid.client.cas import download, upload
from buildgrid._exceptions import InvalidArgumentError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import create_digest, merkle_tree_maker, read_file

from ..cli import pass_context


@click.group(name='cas', short_help="Interact with the CAS server.")
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
@click.option('--instance-name', type=click.STRING, default='main', show_default=True,
              help="Targeted farm instance name.")
@pass_context
def cli(context, remote, instance_name, auth_token, client_key, client_cert, server_cert):
    """Entry point for the bgd-cas CLI command group."""
    try:
        context.channel, _ = setup_channel(remote, auth_token=auth_token,
                                           client_key=client_key, client_cert=client_cert)

    except InvalidArgumentError as e:
        click.echo("Error: {}.".format(e), err=True)
        sys.exit(-1)

    context.instance_name = instance_name


@cli.command('upload-dummy', short_help="Upload a dummy action. Should be used with `execute dummy-request`")
@pass_context
def upload_dummy(context):
    action = remote_execution_pb2.Action(do_not_cache=True)
    with upload(context.channel, instance=context.instance_name) as uploader:
        action_digest = uploader.put_message(action)

    if action_digest.ByteSize():
        click.echo('Success: Pushed digest=["{}/{}]"'
                   .format(action_digest.hash, action_digest.size_bytes))
    else:
        click.echo("Error: Failed pushing empty message.", err=True)


@cli.command('upload-file', short_help="Upload files to the CAS server.")
@click.argument('file_path', nargs=-1, type=click.Path(exists=True, dir_okay=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check uploaded files integrity.")
@pass_context
def upload_file(context, file_path, verify):
    sent_digests, files_map = [], {}
    with upload(context.channel, instance=context.instance_name) as uploader:
        for path in file_path:
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            click.echo("Queueing path=[{}]".format(path))

            file_digest = uploader.upload_file(path, queue=True)

            files_map[file_digest.hash] = path
            sent_digests.append(file_digest)

    for file_digest in sent_digests:
        file_path = os.path.relpath(files_map[file_digest.hash])
        if verify and file_digest.size_bytes != os.stat(file_path).st_size:
            click.echo("Error: Failed to verify '{}'".format(file_path), err=True)
        elif file_digest.ByteSize():
            click.echo("Success: Pushed path=[{}] with digest=[{}/{}]"
                       .format(file_path, file_digest.hash, file_digest.size_bytes))
        else:
            click.echo("Error: Failed pushing path=[{}]".format(file_path), err=True)


@cli.command('upload-dir', short_help="Upload a directory to the CAS server.")
@click.argument('directory-path', nargs=1, type=click.Path(exists=True, file_okay=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check uploaded directory's integrity.")
@pass_context
def upload_directory(context, directory_path, verify):
    sent_digests, nodes_map = [], {}
    with upload(context.channel, instance=context.instance_name) as uploader:
        for node, blob, path in merkle_tree_maker(directory_path):
            if not os.path.isabs(path):
                path = os.path.abspath(path)
            click.echo("Queueing path=[{}]".format(path))

            node_digest = uploader.put_blob(blob, digest=node.digest, queue=True)

            nodes_map[node.digest.hash] = path
            sent_digests.append(node_digest)

    for node_digest in sent_digests:
        node_path = nodes_map[node_digest.hash]
        if not os.path.isabs(directory_path):
            node_path = os.path.relpath(node_path)
        if verify and (os.path.isfile(node_path) and
                       node_digest.size_bytes != os.stat(node_path).st_size):
            click.echo("Error: Failed to verify path=[{}]".format(node_path), err=True)
        elif node_digest.ByteSize():
            click.echo("Success: Pushed path=[{}] with digest=[{}/{}]"
                       .format(node_path, node_digest.hash, node_digest.size_bytes))
        else:
            click.echo("Error: Failed pushing path=[{}]".format(node_path), err=True)


def _create_digest(digest_string):
    digest_hash, digest_size = digest_string.split('/')

    digest = remote_execution_pb2.Digest()
    digest.hash = digest_hash
    digest.size_bytes = int(digest_size)

    return digest


@cli.command('download-file', short_help="Download a file from the CAS server.")
@click.argument('digest-string', nargs=1, type=click.STRING, required=True)
@click.argument('file-path', nargs=1, type=click.Path(exists=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check downloaded file's integrity.")
@pass_context
def download_file(context, digest_string, file_path, verify):
    if os.path.exists(file_path):
        click.echo("Error: Invalid value for " +
                   "path=[{}] already exists.".format(file_path), err=True)
        return

    digest = _create_digest(digest_string)
    with download(context.channel, instance=context.instance_name) as downloader:
        downloader.download_file(digest, file_path)

    if verify:
        file_digest = create_digest(read_file(file_path))
        if file_digest != digest:
            click.echo("Error: Failed to verify path=[{}]".format(file_path), err=True)
            return

    if os.path.isfile(file_path):
        click.echo("Success: Pulled path=[{}] from digest=[{}/{}]"
                   .format(file_path, digest.hash, digest.size_bytes))
    else:
        click.echo('Error: Failed pulling "{}"'.format(file_path), err=True)


@cli.command('download-dir', short_help="Download a directory from the CAS server.")
@click.argument('digest-string', nargs=1, type=click.STRING, required=True)
@click.argument('directory-path', nargs=1, type=click.Path(exists=False), required=True)
@click.option('--verify', is_flag=True, show_default=True,
              help="Check downloaded directory's integrity.")
@pass_context
def download_directory(context, digest_string, directory_path, verify):
    if os.path.exists(directory_path):
        if not os.path.isdir(directory_path) or os.listdir(directory_path):
            click.echo("Error: Invalid value, " +
                       "path=[{}] already exists.".format(directory_path), err=True)
            return

    digest = _create_digest(digest_string)
    with download(context.channel, instance=context.instance_name) as downloader:
        downloader.download_directory(digest, directory_path)

    if verify:
        last_directory_node = None
        for node, _, _ in merkle_tree_maker(directory_path):
            if node.DESCRIPTOR is remote_execution_pb2.DirectoryNode.DESCRIPTOR:
                last_directory_node = node
        if last_directory_node.digest != digest:
            click.echo("Error: Failed to verify path=[{}]".format(directory_path), err=True)
            return

    if os.path.isdir(directory_path):
        click.echo("Success: Pulled path=[{}] from digest=[{}/{}]"
                   .format(directory_path, digest.hash, digest.size_bytes))
    else:
        click.echo("Error: Failed pulling path=[{}]".format(directory_path), err=True)
