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
CommandLineInterface
===================

Any files in the commands/ folder with the name cmd_*.py
will be attempted to be imported.
"""

import os
import logging

import click
import grpc

from buildgrid.utils import read_file

from . import _logging

CONTEXT_SETTINGS = dict(auto_envvar_prefix='BUILDGRID')


class Context:

    def __init__(self):
        self.verbose = False

        self.user_home = os.getcwd()

    def load_client_credentials(self, client_key=None, client_cert=None, server_cert=None):
        """Looks-up and loads TLS client gRPC credentials.

        Args:
            client_key(str): root certificate file path.
            client_cert(str): private key file path.
            server_cert(str): certificate chain file path.

        Returns:
            :obj:`ChannelCredentials`: The credentials for use for a
            TLS-encrypted gRPC client channel.
        """

        if not server_cert or not os.path.exists(server_cert):
            return None

        server_cert_pem = read_file(server_cert)
        if client_key and os.path.exists(client_key):
            client_key_pem = read_file(client_key)
        else:
            client_key_pem = None
            client_key = None
        if client_key_pem and client_cert and os.path.exists(client_cert):
            client_cert_pem = read_file(client_cert)
        else:
            client_cert_pem = None
            client_cert = None

        credentials = grpc.ssl_channel_credentials(root_certificates=server_cert_pem,
                                                   private_key=client_key_pem,
                                                   certificate_chain=client_cert_pem)

        credentials.client_key = client_key
        credentials.client_cert = client_cert
        credentials.server_cert = server_cert

        return credentials

    def load_server_credentials(self, server_key=None, server_cert=None, client_certs=None):
        """Looks-up and loads TLS server gRPC credentials.

        Every private and public keys are expected to be PEM-encoded.

        Args:
            server_key(str): private server key file path.
            server_cert(str): public server certificate file path.
            client_certs(str): public client certificates file path.

        Returns:
            :obj:`ServerCredentials`: The credentials for use for a
            TLS-encrypted gRPC server channel.
        """
        if not server_key or not os.path.exists(server_key):
            return None

        if not server_cert or not os.path.exists(server_cert):
            return None

        server_key_pem = read_file(server_key)
        server_cert_pem = read_file(server_cert)
        if client_certs and os.path.exists(client_certs):
            client_certs_pem = read_file(client_certs)
        else:
            client_certs_pem = None
            client_certs = None

        credentials = grpc.ssl_server_credentials([(server_key_pem, server_cert_pem)],
                                                  root_certificates=client_certs_pem,
                                                  require_client_auth=bool(client_certs))

        credentials.server_key = server_key
        credentials.server_cert = server_cert
        credentials.client_certs = client_certs

        return credentials


pass_context = click.make_pass_decorator(Context, ensure=True)
cmd_folder = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                          'commands'))


class BuildGridCLI(click.MultiCommand):

    def list_commands(self, context):
        commands = []
        for filename in os.listdir(cmd_folder):
            if filename.endswith('.py') and \
               filename.startswith('cmd_'):
                commands.append(filename[4:-3])
        commands.sort()
        return commands

    def get_command(self, context, name):
        mod = __import__(name='buildgrid._app.commands.cmd_{}'.format(name),
                         fromlist=['cli'])
        return mod.cli


@click.command(cls=BuildGridCLI, context_settings=CONTEXT_SETTINGS)
@click.option('-v', '--verbose', is_flag=True,
              help='Enables verbose mode.')
@pass_context
def cli(context, verbose):
    """BuildGrid App"""
    logger = _logging.bgd_logger()
    context.verbose = verbose
    if verbose:
        logger.setLevel(logging.DEBUG)
