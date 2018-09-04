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


import os

import yaml

from buildgrid.server.controller import ExecutionController
from buildgrid.server.actioncache.storage import ActionCache
from buildgrid.server.cas.instance import ByteStreamInstance, ContentAddressableStorageInstance
from buildgrid.server.cas.storage.disk import DiskStorage
from buildgrid.server.cas.storage.lru_memory_cache import LRUMemoryCache
from buildgrid.server.cas.storage.s3 import S3Storage
from buildgrid.server.cas.storage.with_cache import WithCacheStorage


class YamlFactory(yaml.YAMLObject):
    @classmethod
    def from_yaml(cls, loader, node):
        values = loader.construct_mapping(node, deep=True)
        return cls(**values)


class Disk(YamlFactory):

    yaml_tag = u'!disk-storage'

    def __new__(cls, path):
        path = os.path.expanduser(path)
        return DiskStorage(path)


class LRU(YamlFactory):

    yaml_tag = u'!lru-storage'

    def __new__(cls, size):
        return LRUMemoryCache(_parse_size(size))


class S3(YamlFactory):

    yaml_tag = u'!s3-storage'

    def __new__(cls, bucket, endpoint):
        return S3Storage(bucket, endpoint_url=endpoint)


class WithCache(YamlFactory):

    yaml_tag = u'!with-cache-storage'

    def __new__(cls, cache, fallback):
        return WithCacheStorage(cache, fallback)


class Execution(YamlFactory):

    yaml_tag = u'!execution'

    def __new__(cls, storage, action_cache=None):
        return ExecutionController(action_cache, storage)


class Action(YamlFactory):

    yaml_tag = u'!action-cache'

    def __new__(cls, storage, max_cached_refs=0, allow_updates=True):
        return ActionCache(storage, max_cached_refs, allow_updates)


class CAS(YamlFactory):

    yaml_tag = u'!cas'

    def __new__(cls, storage):
        return ContentAddressableStorageInstance(storage)


class ByteStream(YamlFactory):

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

    yaml.SafeLoader.add_constructor(Execution.yaml_tag, Execution.from_yaml)
    yaml.SafeLoader.add_constructor(Execution.yaml_tag, Execution.from_yaml)
    yaml.SafeLoader.add_constructor(Action.yaml_tag, Action.from_yaml)
    yaml.SafeLoader.add_constructor(Disk.yaml_tag, Disk.from_yaml)
    yaml.SafeLoader.add_constructor(LRU.yaml_tag, LRU.from_yaml)
    yaml.SafeLoader.add_constructor(S3.yaml_tag, S3.from_yaml)
    yaml.SafeLoader.add_constructor(WithCache.yaml_tag, WithCache.from_yaml)
    yaml.SafeLoader.add_constructor(CAS.yaml_tag, CAS.from_yaml)
    yaml.SafeLoader.add_constructor(ByteStream.yaml_tag, ByteStream.from_yaml)

    return yaml
