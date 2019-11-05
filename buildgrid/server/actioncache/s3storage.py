# Copyright (C) 2019 Bloomberg LP
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
S3 Action Cache
==================

Implements an Action Cache using S3 to store cache entries.

"""

import collections
import logging
import boto3
import io

from botocore.exceptions import ClientError
from buildgrid._exceptions import NotFoundError
from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from ...utils import get_hash_type


class S3ActionCache:

    def __init__(self, storage, allow_updates=True, cache_failed_actions=True, bucket=None,
                 endpoint=None, access_key=None, secret_key=None):
        """ Initialises a new ActionCache instance using S3 to persist the action cache.

        Args:
            storage (StorageABC): storage backend instance to be used to store ActionResults.
            allow_updates (bool): allow the client to write to storage
            cache_failed_actions (bool): whether to store failed actions in the Action Cache

            bucket (str): Name of bucket
            endpoint (str): URL of endpoint.
            access-key (str): S3-ACCESS-KEY
            secret-key (str): S3-SECRET-KEY
        """
        self.__logger = logging.getLogger(__name__)

        self._instance_name = None

        self.__storage = storage

        self._allow_updates = allow_updates
        self._cache_failed_actions = cache_failed_actions
        self._bucket = bucket

        self._s3cache = boto3.resource('s3', endpoint_url=endpoint, aws_access_key_id=access_key,
                                       aws_secret_access_key=secret_key)

    # --- Public API ---

    @property
    def instance_name(self):
        return self._instance_name

    @property
    def allow_updates(self):
        return self._allow_updates

    def hash_type(self):
        return get_hash_type()

    def register_instance_with_server(self, instance_name, server):
        """Names and registers the action-cache instance with a given server."""
        if self._instance_name is None:
            server.add_action_cache_instance(self, instance_name)

            self._instance_name = instance_name

        else:
            raise AssertionError("Instance already registered")

    def get_action_result(self, action_digest):
        """Retrieves the cached ActionResult for the given Action digest.

        Args:
            action_digest: The digest to get the result for

        Returns:
            The cached ActionResult matching the given key or raises
            NotFoundError.
        """
        storage_digest = self._get_digest_from_cache(action_digest)
        if storage_digest:
            action_result = self.__storage.get_message(storage_digest,
                                                       remote_execution_pb2.ActionResult)

            if action_result is not None:
                if self._action_result_blobs_still_exist(action_result):
                    return action_result

        if self._allow_updates:
            self.__logger.debug("Removing {}/{} from cache due to missing "
                                "blobs in CAS".format(action_digest.hash, action_digest.size_bytes))
            self._delete_key_from_cache(action_digest)

        raise NotFoundError("Key not found: {}/{}".format(action_digest.hash,
                                                          action_digest.size_bytes))

    def update_action_result(self, action_digest, action_result):
        """Stores the result in cache for the given key.

        Args:
            action_digest (Digest): digest of Action to update
            action_result (Digest): digest of ActionResult to store.
        """
        if self._cache_failed_actions or action_result.exit_code == 0:
            result_digest = self.__storage.put_message(action_result)
            self._update_cache_key(action_digest, result_digest.SerializeToString())

            self.__logger.info("Result cached for action [%s/%s]",
                               action_digest.hash, action_digest.size_bytes)

    # --- Private API ---
    def _get_digest_from_cache(self, digest):
        """Get a digest from the reference cache

        Args:
            digest: Action digest to get the associated ActionResult digest for

        Returns:
            The ActionDigest associated with the cached result, or None if the
            digest doesn't exist
        """
        try:
            obj = self._s3cache.Object(self._bucket,
                                       digest.hash + '_' + str(digest.size_bytes))
            return remote_execution_pb2.Digest.FromString(obj.get()['Body'].read())
        except ClientError as e:
            if e.response['Error']['Code'] not in ['404', 'NoSuchKey']:
                raise
            return None

    def _update_cache_key(self, digest, value):
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        obj = self._s3cache.Object(self._bucket,
                                   digest.hash + '_' + str(digest.size_bytes))
        obj.upload_fileobj(io.BytesIO(value))

    def _delete_key_from_cache(self, digest):
        """Remove an entry from the ActionCache

        Args:
            digest: entry to remove from the ActionCache

        Returns:
            None
        """
        if not self._allow_updates:
            raise NotImplementedError("Updating cache not allowed")

        obj = self._s3cache.Object(self._bucket,
                                   digest.hash + '_' + str(digest.size_bytes))
        obj.delete()

    def _action_result_blobs_still_exist(self, action_result):
        """Checks CAS for ActionResult output blobs existance.

        Args:
            action_result (ActionResult): ActionResult to search referenced
            output blobs for.

        Returns:
            True if all referenced blobs are present in CAS, False otherwise.
        """
        blobs_needed = []

        for output_file in action_result.output_files:
            blobs_needed.append(output_file.digest)

        for output_directory in action_result.output_directories:
            blobs_needed.append(output_directory.tree_digest)
            tree = self.__storage.get_message(output_directory.tree_digest,
                                              remote_execution_pb2.Tree)
            if tree is None:
                return False

            for file_node in tree.root.files:
                blobs_needed.append(file_node.digest)

            for child in tree.children:
                for file_node in child.files:
                    blobs_needed.append(file_node.digest)

        if action_result.stdout_digest.hash and not action_result.stdout_raw:
            blobs_needed.append(action_result.stdout_digest)

        if action_result.stderr_digest.hash and not action_result.stderr_raw:
            blobs_needed.append(action_result.stderr_digest)

        missing = self.__storage.missing_blobs(blobs_needed)
        if len(missing) != 0:
            self.__logger.debug("Missing {}/{} blobs".format(len(missing), len(blobs_needed)))
            return False
        return True
