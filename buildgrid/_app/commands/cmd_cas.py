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
import sys
from urllib.parse import urlparse

import click
import grpc

from buildgrid.utils import merkle_maker, create_digest
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2, remote_execution_pb2_grpc

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


@cli.command('upload-files', short_help="Upload files to the CAS server.")
@click.argument('files', nargs=-1, type=click.File('rb'), required=True)
@pass_context
def upload_files(context, files):
    stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(context.channel)

    requests = []
    for file in files:
        chunk = file.read()
        requests.append(remote_execution_pb2.BatchUpdateBlobsRequest.Request(
            digest=create_digest(chunk), data=chunk))

    request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name=context.instance_name,
                                                           requests=requests)

    context.logger.info("Sending: {}".format(request))
    response = stub.BatchUpdateBlobs(request)
    context.logger.info("Response: {}".format(response))


@cli.command('upload-dir', short_help="Upload a directory to the CAS server.")
@click.argument('directory', nargs=1, type=click.Path(), required=True)
@pass_context
def upload_dir(context, directory):
    context.logger.info("Uploading directory to cas")
    stub = remote_execution_pb2_grpc.ContentAddressableStorageStub(context.channel)

    requests = []

    for chunk, file_digest in merkle_maker(directory):
        requests.append(remote_execution_pb2.BatchUpdateBlobsRequest.Request(
            digest=file_digest, data=chunk))

    request = remote_execution_pb2.BatchUpdateBlobsRequest(instance_name=context.instance_name,
                                                           requests=requests)

    context.logger.info("Request:\n{}".format(request))
    response = stub.BatchUpdateBlobs(request)
    context.logger.info("Response:\n{}".format(response))
