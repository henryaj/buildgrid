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
DiskStorage
==================

A CAS storage provider that stores files as blobs on disk.
"""

import os
import tempfile

from .storage_abc import StorageABC


class DiskStorage(StorageABC):

    def __init__(self, path):
        if not os.path.isabs(path):
            self.__root_path = os.path.abspath(path)
        else:
            self.__root_path = path
        self.__cas_path = os.path.join(self.__root_path, 'cas')

        self.objects_path = os.path.join(self.__cas_path, 'objects')
        self.temp_path = os.path.join(self.__root_path, 'tmp')

        os.makedirs(self.objects_path, exist_ok=True)
        os.makedirs(self.temp_path, exist_ok=True)

    def has_blob(self, digest):
        return os.path.exists(self._get_object_path(digest))

    def get_blob(self, digest):
        try:
            return open(self._get_object_path(digest), 'rb')
        except FileNotFoundError:
            return None

    def begin_write(self, digest):
        return tempfile.NamedTemporaryFile("wb", dir=self.temp_path)

    def commit_write(self, digest, write_session):
        object_path = self._get_object_path(digest)

        try:
            os.makedirs(os.path.dirname(object_path), exist_ok=True)
            os.link(write_session.name, object_path)
        except FileExistsError:
            # Object is already there!
            pass

        write_session.close()

    def _get_object_path(self, digest):
        return os.path.join(self.objects_path, digest.hash[:2], digest.hash[2:])
