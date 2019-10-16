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
import queue

from grpc._server import _Context

from buildgrid.server.execution.instance import ExecutionInstance
from buildgrid._exceptions import FailedPreconditionError
from buildgrid._protos.google.longrunning import operations_pb2


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


@pytest.fixture
def mock_exec_instance():
    return ExecutionInstance(
        MockScheduler(), MockStorage([]), [])


@pytest.fixture
def mock_active_context():
    cxt = mock.MagicMock(spec=_Context)
    yield cxt


@pytest.fixture
def mock_operation():
    operation = mock.Mock(spec=operations_pb2.Operation)
    operation.done = False
    return operation


@pytest.fixture
def mock_operation_done():
    operation = mock.Mock(spec=operations_pb2.Operation)
    operation.done = True
    return operation


@pytest.fixture(params=[0, 1, 2])
def operation_updates_message_seq(mock_operation, request):
    seq = []
    for i in range(request.param):
        seq.append((None, mock_operation))

    return (request.param, seq)


@pytest.fixture(params=[0, 1, 2])
def operation_updates_completing_message_seq(mock_operation, mock_operation_done, request):
    seq = []
    for i in range(request.param):
        seq.append((None, mock_operation))

    seq.append((None, mock_operation_done))

    # Add sentinel operation update: this should never be dequeued
    # since we should stop once the operation completed (above)
    seq.append((ValueError, mock_operation))
    return (request.param, seq)


@pytest.fixture(params=[0, 1, 2])
def operation_updates_ending_with_error_seq(mock_operation, mock_operation_done, request):
    seq = []
    for i in range(request.param):
        seq.append((None, mock_operation))

    seq.append((RuntimeError, mock_operation))

    # Add sentinel operation update: this should never be dequeued
    # since we should stop once we encounter the first error
    seq.append((ValueError, mock_operation))
    return (request.param, seq)


def test_stream_operation_updates_stopsiteration_when_operation_done(operation_updates_completing_message_seq,
                                                                     mock_exec_instance, mock_active_context):
    """
        Make sure that `stream_operation_updates_while_context_is_active` raises
        a StopIteration once the operation has completed
    """
    n, msg_seq = operation_updates_completing_message_seq
    message_queue = queue.Queue()
    for msg_err, msg_operation in msg_seq:
        message_queue.put((msg_err, msg_operation))

    generator = mock_exec_instance.stream_operation_updates_while_context_is_active(
        message_queue, mock_active_context, timeout=0)

    for i in range(n):
        seq_error, seq_operation = msg_seq[i]
        operation = next(generator)

        assert seq_operation == operation
        assert not operation.done

    # Operation completion
    seq_error, seq_operation = msg_seq[n]
    operation = next(generator)

    assert seq_operation == operation
    assert operation.done

    # After operation completion
    with pytest.raises(StopIteration):
        next(generator)


@pytest.fixture(params=[0, 1, 2, 3])
def n_0_to_3_inclusive(request):
    return request.param


def test_stream_operation_updates_stopsiteration_when_context_becomes_inactive(mock_operation,
                                                                               mock_exec_instance, mock_active_context,
                                                                               n_0_to_3_inclusive):
    """
        Make sure that `stream_operation_updates_while_context_is_active` raises a StopIteration
        once the context closes even when there are more updates
    """
    numberOfYieldsBeforeContextClose = n_0_to_3_inclusive

    # Add more items than we'll yield before the context closes
    message_queue = queue.Queue()
    for i in range(numberOfYieldsBeforeContextClose * 2):
        message_queue.put((None, mock_operation))

    generator = mock_exec_instance.stream_operation_updates_while_context_is_active(
        message_queue, mock_active_context, timeout=0)

    # get first n
    for i in range(numberOfYieldsBeforeContextClose):
        operation = next(generator)
        assert operation == mock_operation

    # "Close" the context...
    mock_active_context.is_active = mock.MagicMock(return_value=False)

    # see that we get StopIteration right after closing the context
    with pytest.raises(StopIteration):
        next(generator)


def test_stream_operation_raises_and_stopsiteration_on_operation_errors(operation_updates_ending_with_error_seq,
                                                                        mock_exec_instance, mock_active_context):
    """
        Make sure that `stream_operation_updates_while_context_is_active` raises
        operation update errors and then ends (raises StopIteration)
    """
    n, msg_seq = operation_updates_ending_with_error_seq
    message_queue = queue.Queue()
    for msg_err, msg_operation in msg_seq:
        message_queue.put((msg_err, msg_operation))

    generator = mock_exec_instance.stream_operation_updates_while_context_is_active(
        message_queue, mock_active_context, timeout=0)

    for i in range(n):
        seq_error, seq_operation = msg_seq[i]
        operation = next(generator)

        assert seq_operation == operation
        assert not operation.done

    with pytest.raises(RuntimeError):
        operation = next(generator)

    with pytest.raises(StopIteration):
        operation = next(generator)
