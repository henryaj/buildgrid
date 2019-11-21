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


from inspect import getfullargspec
import os
import sys
from urllib.parse import urlparse

import click
import grpc
import yaml

from buildgrid.server.controller import ExecutionController
from buildgrid.server.actioncache.instance import ActionCache
from buildgrid.server.actioncache.remote import RemoteActionCache
from buildgrid.server.actioncache.s3storage import S3ActionCache
from buildgrid.server.referencestorage.storage import ReferenceCache
from buildgrid.server.cas.instance import ByteStreamInstance, ContentAddressableStorageInstance
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.remote import RemoteStorage
from buildgrid.server.cas.storage.s3 import S3Storage
from buildgrid.server.cas.storage.gcs import GCSStorage
from buildgrid.server.cas.storage.with_cache import WithCacheStorage
from buildgrid.server.cas.storage.index.sql import SQLIndex
from buildgrid.server.persistence.mem.impl import MemoryDataStore
from buildgrid.server.persistence.sql.impl import SQLDataStore

from ..cli import Context
from ..._enums import DataStoreType


class YamlFactory(yaml.YAMLObject):
    """ Base class for contructing maps or scalars from tags.
    """

    @classmethod
    def from_yaml(cls, loader, node):
        if isinstance(node, yaml.ScalarNode):
            value = loader.construct_scalar(node)
            return cls(value)

        else:
            values = loader.construct_mapping(node, deep=True)
            for key, value in dict(values).items():
                values[key.replace('-', '_')] = values.pop(key)
            return cls(**values)


class Channel(YamlFactory):
    """Creates a GRPC channel.

    The :class:`Channel` class returns a `grpc.Channel` and is generated from the tag ``!channel``.
    Creates either a secure or insecure channel.

    Args:
       port (int): A port for the channel.
       insecure_mode (bool): If ``True``, generates an insecure channel, even if there are
    credentials. Defaults to ``True``.
       credentials (dict, optional): A dictionary in the form::

           tls-server-key: /path/to/server-key
           tls-server-cert: /path/to/server-cert
           tls-client-certs: /path/to/client-certs
    """

    yaml_tag = u'!channel'

    def __init__(self, port, insecure_mode, credentials=None):
        self.address = '[::]:{0}'.format(port)
        self.credentials = None

        context = Context()

        if not insecure_mode:
            server_key = credentials['tls-server-key']
            server_cert = credentials['tls-server-cert']
            client_certs = credentials['tls-client-certs']
            self.credentials = context.load_server_credentials(server_key, server_cert, client_certs)

            if not self.credentials:
                click.echo("ERROR: load_server_credentials could not find certificates.\n" +
                           "Please check whether the specified certificate paths exist.\n", err=True)
                sys.exit(-1)


class ExpandPath(YamlFactory):
    """Returns a string of the user's path after expansion.

    The :class:`ExpandPath` class returns a string and is generated from the tag ``!expand-path``.

    Args:
       path (str): Can be used with strings such as: ``~/dir/to/something`` or ``$HOME/certs``
    """

    yaml_tag = u'!expand-path'

    def __new__(cls, path):
        path = os.path.expanduser(path)
        path = os.path.expandvars(path)
        return path


class ReadFile(YamlFactory):
    """Returns a string of the contents of the specified file.

    The :class:`ReadFile` class returns a string and is generated from the tag ``!read-file``.

    Args:
       path (str): Can be used with strings such as: ``~/path/to/some/file`` or ``$HOME/myfile`` or ``/path/to/file``
    """

    yaml_tag = u'!read-file'

    def __new__(cls, path):
        # Expand path
        path = os.path.expanduser(path)
        path = os.path.expandvars(path)

        if not os.path.exists(path):
            click.echo("ERROR: read-file `{}` failed due to it not existing or bad permissions.".format(path),
                       err=True)
            sys.exit(-1)
        else:
            with open(path, 'r') as file:
                try:
                    file_contents = "\n".join(file.readlines()).strip()
                    return file_contents
                except IOError as e:
                    click.echo("ERROR: read-file failed to read file `{}`: {}".format(path, e), err=True)
                    sys.exit(-1)


class Disk(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.disk.DiskStorage` using the tag ``!disk-storage``.

    Args:
       path (str): Path to directory to storage.

    """

    yaml_tag = u'!disk-storage'

    def __new__(cls, path):
        """Creates a new disk

        Args:
           path (str): Some path
        """
        return DiskStorage(path)


class LRU(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.lru_memory_cache.LRUMemoryCache` using the tag ``!lru-storage``.

    Args:
      size (int): Size e.g ``10kb``. Size parsed with :meth:`buildgrid._app.settings.parser._parse_size`.
    """

    yaml_tag = u'!lru-storage'

    def __new__(cls, size):
        return LRUMemoryCache(_parse_size(size))


class S3(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.s3.S3Storage` using the tag ``!s3-storage``.

    Args:
        bucket (str): Name of bucket
        endpoint (str): URL of endpoint.
        access-key (str): S3-ACCESS-KEY
        secret-key (str): S3-SECRET-KEY
    """

    yaml_tag = u'!s3-storage'

    def __new__(cls, bucket, endpoint, access_key, secret_key):
        return S3Storage(bucket, endpoint_url=endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)


class GCS(YamlFactory):
    """
    Generates :class:`buildgrid.server.cas.storage.gcs.GCSStorage` using the
    tag ``!gcs-storage``.
    """

    yaml_tag = u'!gcs-storage'

    def __new__(cls, bucket):
        return GCSStorage(bucket)


class Redis(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.redis.RedisStorage` using the tag ``!redis-storage``.

    Args:
        host (str): hostname of endpoint.
        port (int): port on host.
        password (str): redis database password
        db (int) : db number
    """

    yaml_tag = u'!redis-storage'

    def __new__(cls, host, port, password=None, db=None):
        # Import here so there is no global buildgrid dependency on redis
        from buildgrid.server.cas.storage.redis import RedisStorage
        return RedisStorage(host=host, port=port, password=password, db=db)


class Remote(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.remote.RemoteStorage`
    using the tag ``!remote-storage``.

    Args:
      url (str): URL to remote storage. If used with ``https``, needs credentials.
      instance_name (str): Instance of the remote to connect to.
      credentials (dict, optional): A dictionary in the form::

           tls-client-key: /path/to/client-key
           tls-client-cert: /path/to/client-cert
           tls-server-cert: /path/to/server-cert
    """

    yaml_tag = u'!remote-storage'

    def __new__(cls, url, instance_name, credentials=None):
        # TODO: Context could be passed into the parser.
        # Also find way to get instance_name from parent
        # Issue 82
        context = Context()

        url = urlparse(url)
        remote = '{}:{}'.format(url.hostname, url.port or 50051)

        channel = None
        if url.scheme == 'http':
            channel = grpc.insecure_channel(remote)

        else:
            if not credentials:
                click.echo("ERROR: no TLS keys were specified and no defaults could be found.\n" +
                           "Set remote url scheme to `http` in order to deactivate" +
                           "TLS encryption.\n", err=True)
                sys.exit(-1)

            client_key = credentials['tls-client-key']
            client_cert = credentials['tls-client-cert']
            server_cert = credentials['tls-server-cert']
            credentials = context.load_client_credentials(client_key,
                                                          client_cert,
                                                          server_cert)
            if not credentials:
                click.echo("ERROR: no TLS keys were specified and no defaults could be found.\n" +
                           "Set remote url scheme to `http` in order to deactivate" +
                           "TLS encryption.\n", err=True)
                sys.exit(-1)

            channel = grpc.secure_channel(remote, credentials)

        return RemoteStorage(channel, instance_name)


class WithCache(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.with_cache.WithCacheStorage`
    using the tag ``!with-cache-storage``.

    Args:
      url (str): URL to remote storage. If used with ``https``, needs credentials.
      instance_name (str): Instance of the remote to connect to.
      credentials (dict, optional): A dictionary in the form::

           tls-client-key: /path/to/certs
           tls-client-cert: /path/to/certs
           tls-server-cert: /path/to/certs
    """

    yaml_tag = u'!with-cache-storage'

    def __new__(cls, cache, fallback):
        return WithCacheStorage(cache, fallback)


class SQLDataStoreConfig(YamlFactory):

    yaml_tag = u'!sql-data-store'

    def __new__(cls, storage, connection_string=None, automigrate=False,
                **kwargs):
        try:
            return SQLDataStore(storage,
                                connection_string=connection_string,
                                automigrate=automigrate,
                                **kwargs)
        except TypeError as type_error:
            click.echo(type_error, err=True)
            sys.exit(-1)


class MemoryDataStoreConfig(YamlFactory):

    yaml_tag = u'!memory-data-store'

    def __new__(cls, storage):
        return MemoryDataStore(storage)


class SQL_Index(YamlFactory):
    """Generates :class:`buildgrid.server.cas.storage.index.sql.SQLIndex`
    using the tag ``!sql-index``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
      connection_string (str): SQLAlchemy connection string
      automigrate (bool): Attempt to automatically upgrade an existing DB schema to the newest version.
      window_size (uint): Maximum number of blobs to fetch in one SQL operation (larger resultsets will
        be automatically split into multiple queries)
      inclause_limit (int): If nonnegative, overrides the default number of variables permitted per "in"
        clause. See the buildgrid.server.cas.storage.index.sql.SQLIndex comments for more details.
      fallback_on_get (bool): By default, the SQL Index only fetches blobs from the underlying storage
        if they're present in the index on get_blob/bulk_read_blobs requests to minimize interactions
        with the storage. If this is set, the index instead checks the underlying storage directly on
        get_blob/bulk_read_blobs requests, then loads all blobs found into the index.
    """

    yaml_tag = u'!sql-index'

    def __new__(cls, storage, connection_string, automigrate=False,
                window_size=1000, inclause_limit=-1, fallback_on_get=False, **kwargs):
        return SQLIndex(storage=storage, connection_string=connection_string,
                        automigrate=automigrate, window_size=window_size,
                        inclause_limit=inclause_limit,
                        fallback_on_get=fallback_on_get, **kwargs)


class Execution(YamlFactory):
    """Generates :class:`buildgrid.server.execution.service.ExecutionService`
    using the tag ``!execution``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
      action_cache(:class:`Action`): Instance of action cache to use.
      action_browser_url The base URL to use to generate Action Browser links to users
      data_store The DataStore options (see class `DataStore(YamlFactory)` for options)
      property_keys The allowed property keys for jobs
      bot_session_keepalive_timeout: The longest time (in seconds) we'll wait for a bot to send an update
    before it assumes it's dead. Defaults to 600s (10 minutes).
    """

    yaml_tag = u'!execution'

    def __new__(cls, storage, action_cache=None, action_browser_url=None, data_store=None,
                property_keys=None, bot_session_keepalive_timeout=600):
        # If the configuration doesn't define a data store type, fallback to the
        # in-memory data store implementation from the old scheduler.
        if not data_store:
            data_store = MemoryDataStore(storage)

        click.echo("Using %s to store state" % data_store)

        return ExecutionController(data_store, storage=storage, action_cache=action_cache,
                                   action_browser_url=action_browser_url, property_keys=property_keys,
                                   bot_session_keepalive_timeout=bot_session_keepalive_timeout)


class Action(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.service.ActionCacheService`
    using the tag ``!action-cache``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
      max_cached_refs(int): Max number of cached actions.
      allow_updates(bool): Allow updates pushed to CAS. Defaults to ``True``.
      cache_failed_actions(bool): Whether to store failed (non-zero exit code) actions. Default to ``True``.
    """

    yaml_tag = u'!action-cache'

    def __new__(cls, storage, max_cached_refs, allow_updates=True, cache_failed_actions=True):
        return ActionCache(storage, max_cached_refs, allow_updates, cache_failed_actions)


class S3Action(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.service.S3ActionCache`
    using the tag ``!s3-action-cache``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
      allow_updates(bool): Allow updates pushed to CAS. Defaults to ``True``.
      cache_failed_actions(bool): Whether to store failed (non-zero exit code) actions. Default to ``True``.
      bucket (str): Name of bucket
      endpoint (str): URL of endpoint.
      access-key (str): S3-ACCESS-KEY
      secret-key (str): S3-SECRET-KEY

    """

    yaml_tag = u'!s3action-cache'

    def __new__(cls, storage, allow_updates=True, cache_failed_actions=True,
                bucket=None, endpoint=None, access_key=None, secret_key=None):
        return S3ActionCache(storage, allow_updates=allow_updates, cache_failed_actions=cache_failed_actions,
                             bucket=bucket, endpoint=endpoint, access_key=access_key, secret_key=secret_key)


class RemoteAction(YamlFactory):
    """Generates :class:`buildgrid.server.actioncache.remote.RemoteActionCache`
    using the tag ``!remote-action-cache``.

    Args:
      url (str): URL to remote action cache. If used with ``https``, needs credentials.
      instance_name (str): Instance of the remote to connect to.
      credentials (dict, optional): A dictionary in the form::

           tls-client-key: /path/to/client-key
           tls-client-cert: /path/to/client-cert
           tls-server-cert: /path/to/server-cert
    """

    yaml_tag = u'!remote-action-cache'

    def __new__(cls, url, instance_name, credentials=None):
        # TODO: Context could be passed into the parser.
        # Also find way to get instance_name from parent
        # Issue 82
        context = Context()

        url = urlparse(url)
        remote = '{}:{}'.format(url.hostname, url.port or 50051)

        channel = None
        if url.scheme == 'http':
            channel = grpc.insecure_channel(remote)

        else:
            if not credentials:
                click.echo("ERROR: no TLS keys were specified and no defaults could be found.\n" +
                           "Set remote url scheme to `http` in order to deactivate" +
                           "TLS encryption.\n", err=True)
                sys.exit(-1)

            client_key = credentials['tls-client-key']
            client_cert = credentials['tls-client-cert']
            server_cert = credentials['tls-server-cert']
            credentials = context.load_client_credentials(client_key,
                                                          client_cert,
                                                          server_cert)
            if not credentials:
                click.echo("ERROR: no TLS keys were specified and no defaults could be found.\n" +
                           "Set remote url scheme to `http` in order to deactivate" +
                           "TLS encryption.\n", err=True)
                sys.exit(-1)

            channel = grpc.secure_channel(remote, credentials)

        return RemoteActionCache(channel, instance_name)


class Reference(YamlFactory):
    """Generates :class:`buildgrid.server.referencestorage.service.ReferenceStorageService`
    using the tag ``!reference-cache``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
      max_cached_refs(int): Max number of cached actions.
      allow_updates(bool): Allow updates pushed to CAS. Defauled to ``True``.
    """

    yaml_tag = u'!reference-cache'

    def __new__(cls, storage, max_cached_refs, allow_updates=True):
        return ReferenceCache(storage, max_cached_refs, allow_updates)


class CAS(YamlFactory):
    """Generates :class:`buildgrid.server.cas.service.ContentAddressableStorageService`
    using the tag ``!cas``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
    """

    yaml_tag = u'!cas'

    def __new__(cls, storage):
        return ContentAddressableStorageInstance(storage)


class ByteStream(YamlFactory):
    """Generates :class:`buildgrid.server.cas.service.ByteStreamService`
    using the tag ``!bytestream``.

    Args:
      storage(:class:`buildgrid.server.cas.storage.storage_abc.StorageABC`): Instance of storage to use.
    """

    yaml_tag = u'!bytestream'

    def __new__(cls, storage):
        return ByteStreamInstance(storage)


def _parse_size(size):
    """Convert a string containing a size in bytes (e.g. '2GB') to a number."""
    _size_prefixes = {'k': 2 ** 10, 'm': 2 ** 20, 'g': 2 ** 30, 't': 2 ** 40}
    size = size.lower()

    if size[-1] == 'b':
        size = size[:-1]
    if size[-1] in _size_prefixes:
        return int(size[:-1]) * _size_prefixes[size[-1]]
    return int(size)


def get_parser():

    yaml.SafeLoader.add_constructor(Channel.yaml_tag, Channel.from_yaml)
    yaml.SafeLoader.add_constructor(ExpandPath.yaml_tag, ExpandPath.from_yaml)
    yaml.SafeLoader.add_constructor(ReadFile.yaml_tag, ReadFile.from_yaml)
    yaml.SafeLoader.add_constructor(Execution.yaml_tag, Execution.from_yaml)
    yaml.SafeLoader.add_constructor(Action.yaml_tag, Action.from_yaml)
    yaml.SafeLoader.add_constructor(RemoteAction.yaml_tag, RemoteAction.from_yaml)
    yaml.SafeLoader.add_constructor(S3Action.yaml_tag, S3Action.from_yaml)
    yaml.SafeLoader.add_constructor(Reference.yaml_tag, Reference.from_yaml)
    yaml.SafeLoader.add_constructor(Disk.yaml_tag, Disk.from_yaml)
    yaml.SafeLoader.add_constructor(LRU.yaml_tag, LRU.from_yaml)
    yaml.SafeLoader.add_constructor(S3.yaml_tag, S3.from_yaml)
    yaml.SafeLoader.add_constructor(GCS.yaml_tag, GCS.from_yaml)
    yaml.SafeLoader.add_constructor(Redis.yaml_tag, Redis.from_yaml)
    yaml.SafeLoader.add_constructor(Remote.yaml_tag, Remote.from_yaml)
    yaml.SafeLoader.add_constructor(WithCache.yaml_tag, WithCache.from_yaml)
    yaml.SafeLoader.add_constructor(SQL_Index.yaml_tag, SQL_Index.from_yaml)
    yaml.SafeLoader.add_constructor(CAS.yaml_tag, CAS.from_yaml)
    yaml.SafeLoader.add_constructor(ByteStream.yaml_tag, ByteStream.from_yaml)
    yaml.SafeLoader.add_constructor(SQLDataStoreConfig.yaml_tag, SQLDataStoreConfig.from_yaml)
    yaml.SafeLoader.add_constructor(MemoryDataStoreConfig.yaml_tag, MemoryDataStoreConfig.from_yaml)

    return yaml
