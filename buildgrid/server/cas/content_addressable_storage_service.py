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
ContentAddressableStorageService
==================

Implements the Content Addressable Storage API, which provides methods
to check for missing CAS blobs and update them in bulk.
"""

import grpc
from google.devtools.remoteexecution.v1test import remote_execution_pb2 as re_pb2, remote_execution_pb2_grpc as re_pb2_grpc

class ContentAddressableStorageService(re_pb2_grpc.ContentAddressableStorageServicer):

    def __init__(self, storage):
        self._storage = storage

    def FindMissingBlobs(self, request, context):
        # Only one instance for now.
        storage = self._storage
        return re_pb2.FindMissingBlobsResponse(
            missing_blob_digests=storage.missing_blobs(request.blob_digests))

    def BatchUpdateBlobs(self, request, context):
        # Only one instance for now.
        storage = self._storage
        requests = []
        for request_proto in request.requests:
            requests.append((request_proto.content_digest, request_proto.data))
        response = re_pb2.BatchUpdateBlobsResponse()
        for (digest, _), status in zip(requests, storage.bulk_update_blobs(requests)):
            response_proto = response.responses.add()
            response_proto.blob_digest.CopyFrom(digest)
            response_proto.status.CopyFrom(status)
        return response
