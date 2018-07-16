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
S3Storage
==================

A storage provider that stores data in an Amazon S3 bucket.
"""

import io

import boto3
from botocore.exceptions import ClientError

from .storage_abc import StorageABC

class S3Storage(StorageABC):

    def __init__(self, bucket, **kwargs):
        self._bucket = bucket
        self._s3 = boto3.resource('s3', **kwargs)

    def has_blob(self, digest):
        try:
            self._s3.Object(self._bucket, digest.hash + '_' + str(digest.size_bytes)).load()
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return False
        return True

    def get_blob(self, digest):
        try:
            obj = self._s3.Object(self._bucket, digest.hash + '_' + str(digest.size_bytes))
            return obj.get()['Body']
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return None

    def begin_write(self, _digest):
        # TODO use multipart API for large blobs?
        return io.BytesIO()

    def commit_write(self, digest, write_session):
        write_session.seek(0)
        self._s3.Bucket(self._bucket).upload_fileobj(write_session,
                                                     digest.hash + '_' + str(digest.size_bytes))
        write_session.close()
