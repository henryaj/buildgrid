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


from urllib.parse import urlparse

import pytest

from buildgrid._protos.build.bazel.remote.execution.v2 import remote_execution_pb2
from buildgrid.utils import BrowserURL
from buildgrid.utils import get_hash_type
from buildgrid.utils import create_digest, parse_digest


BLOBS = (b'', b'non-empty-blob',)
BLOB_HASHES = (
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    '89070dfb3175a2c75835d70147b52bd97afd8228819566d84eecd2d20e9b19fc',)
BLOB_SIZES = (0, 14,)
BLOB_DATA = zip(BLOBS, BLOB_HASHES, BLOB_SIZES)

STRINGS = (
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0',
    '89070dfb3175a2c75835d70147b52bd97afd8228819566d84eecd2d20e9b19fc/14',
    'e1ca41574914ba00e8ed5c8fc78ec8efdfd48941c7e48ad74dad8ada7f2066d/12', )
BLOB_HASHES = (
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    '89070dfb3175a2c75835d70147b52bd97afd8228819566d84eecd2d20e9b19fc',
    None, )
BLOB_SIZES = (0, 14, None,)
STRING_VALIDITIES = (True, True, False,)
STRING_DATA = zip(STRINGS, BLOB_HASHES, BLOB_SIZES, STRING_VALIDITIES)

BASE_URL = 'http://localhost:8080'
INSTANCES = (None, '', 'instance',)
URL_HASHES = (
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    '89070dfb3175a2c75835d70147b52bd97afd8228819566d84eecd2d20e9b19fc',)
URL_SIZES = (0, 14,)
URL_DATA = zip(URL_HASHES, URL_SIZES)


@pytest.mark.parametrize('blob,digest_hash,digest_size', BLOB_DATA)
def test_create_digest(blob, digest_hash, digest_size):
    # Generate a Digest message from given blob:
    blob_digest = create_digest(blob)

    assert get_hash_type() == remote_execution_pb2.SHA256

    assert hasattr(blob_digest, 'DESCRIPTOR')
    assert blob_digest.DESCRIPTOR == remote_execution_pb2.Digest.DESCRIPTOR
    assert blob_digest.hash == digest_hash
    assert blob_digest.size_bytes == digest_size


@pytest.mark.parametrize('string,digest_hash,digest_size,validity', STRING_DATA)
def test_parse_digest(string, digest_hash, digest_size, validity):
    # Generate a Digest message from given string:
    string_digest = parse_digest(string)

    assert get_hash_type() == remote_execution_pb2.SHA256

    if validity:
        assert hasattr(string_digest, 'DESCRIPTOR')
        assert string_digest.DESCRIPTOR == remote_execution_pb2.Digest.DESCRIPTOR
        assert string_digest.hash == digest_hash
        assert string_digest.size_bytes == digest_size

    else:
        assert string_digest is None


@pytest.mark.parametrize('instance', INSTANCES)
@pytest.mark.parametrize('digest_hash,digest_size', URL_DATA)
def test_browser_url_initialization(instance, digest_hash, digest_size):
    # Initialize and generate a browser compatible URL:
    browser_url = BrowserURL(BASE_URL, instance)
    browser_digest = remote_execution_pb2.Digest(hash=digest_hash,
                                                 size_bytes=digest_size)

    assert browser_url.generate() is None
    assert browser_url.for_message('type', browser_digest)
    assert not browser_url.for_message(None, None)

    url = browser_url.generate()

    assert url is not None

    parsed_url = urlparse(url)

    if instance:
        assert parsed_url.path.find(instance)
    assert parsed_url.path.find('type') > 0
    assert parsed_url.path.find(digest_hash) > 0
    assert parsed_url.path.find(str(digest_size)) > 0
