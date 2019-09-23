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

from unittest import mock
from collections import namedtuple
import pytest

from buildgrid.server.execution.instance import ExecutionInstance
from buildgrid._exceptions import FailedPreconditionError


class MockScheduler:
    def queue_job_action(self, *args, **kwargs):
        return "queued"


class MockStorage:
    def __init__(self, pairs):
        Command = namedtuple('Command', 'platform command_digest')
        Platform = namedtuple('Platform', 'properties')
        Properties = namedtuple('Properties', 'name value')

        self.properties = [Properties(name=x[0], value=x[1]) for x in pairs]
        self.platform = Platform(properties=self.properties)
        self.command = Command(platform=self.platform, command_digest="fake")

    def get_message(self, *args, **kwargs):
        return self.command


def test_execute_platform_matching_simple():
    """Will match on standard keys."""

    pairs = [("OSFamily", "linux"), ("ISA", "x86-64"), ("ISA", "x86-avx")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), [])
    assert exec_instance.execute("fake", False) == "queued"


def test_execute_platform_matching_too_many_os():
    """Will not match due to too many OSFamilies being specified."""

    pairs = [("OSFamily", "linux"), ("OSFamily", "macos"),
             ("ISA", "x86-64"), ("ISA", "x86-avx")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), [])
    with pytest.raises(FailedPreconditionError):
        exec_instance.execute("fake", False)


def test_execute_platform_matching_too_many_os_platform_key():
    """Make sure adding standard keys to platform keys won't cause issues ."""

    pairs = [("OSFamily", "linux"), ("OSFamily", "macos"),
             ("ISA", "x86-64"), ("ISA", "x86-avx")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), ["OSFamily"])
    with pytest.raises(FailedPreconditionError):
        exec_instance.execute("fake", False)


def test_execute_platform_matching_failure():
    """Will not match due to platform-keys missing 'ChrootDigest'."""

    pairs = [("OSFamily", "linux"), ("ISA", "x86-64"),
             ("ChrootDigest", "deadbeef")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), [])
    with pytest.raises(FailedPreconditionError):
        exec_instance.execute("fake", False)


def test_execute_platform_matching_success():
    """Will match due to platform keys matching."""

    pairs = [("ChrootDigest", "deadbeed")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), ["ChrootDigest"])
    assert exec_instance.execute("fake", False) == "queued"


def test_execute_platform_matching_both_empty():
    """Edge case where nothing specified on either side."""

    pairs = []
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), None)
    assert exec_instance.execute("fake", False) == "queued"


def test_execute_platform_matching_no_job_req():
    """If job doesn't specify platform key requirements, it should always pass."""

    pairs = [("OSFamily", "linux"), ("ISA", "x86-64")]
    exec_instance = ExecutionInstance(
        MockScheduler(), MockStorage(pairs), ["ChrootDigest"])
    assert exec_instance.execute("fake", False) == "queued"
