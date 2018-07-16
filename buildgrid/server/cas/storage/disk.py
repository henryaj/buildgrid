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
#
# Authors:
#        Carter Sande <csande@bloomberg.net>

"""
DiskStorage
==================

A CAS storage provider that stores files as blobs on disk.
"""

import os
import pathlib
import tempfile

from .storage_abc import StorageABC

class DiskStorage(StorageABC):

    def __init__(self, path):
        self._path = pathlib.Path(path)
        os.makedirs(self._path / "temp", exist_ok=True)

    def has_blob(self, digest):
        return (self._path / (digest.hash + "_" + str(digest.size_bytes))).exists()

    def get_blob(self, digest):
        try:
            return open(self._path / (digest.hash + "_" + str(digest.size_bytes)), 'rb')
        except FileNotFoundError:
            return None

    def begin_write(self, _digest):
        return tempfile.NamedTemporaryFile("wb", dir=str(self._path / "temp"))

    def commit_write(self, digest, write_session):
        # Atomically move the temporary file into place.
        path = self._path / (digest.hash + "_" + str(digest.size_bytes))
        os.replace(write_session.name, path)
        try:
            write_session.close()
        except FileNotFoundError:
            # We moved the temporary file to a new location, so when Python
            # tries to delete its old location, it'll fail.
            pass
