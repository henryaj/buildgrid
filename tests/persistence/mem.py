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
#
# pylint: disable=redefined-outer-name


from datetime import datetime
import os
import tempfile

import pytest

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.job import Job
from buildgrid.server.persistence.mem.impl import MemoryDataStore


@pytest.fixture()
def datastore():
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    data_store = MemoryDataStore(storage)
    data_store.activate_monitoring()
    yield data_store


def populate_datastore(datastore):
    datastore.create_job(Job(
        True,
        Digest(hash="test-action", size_bytes=100),
        name="test-job",
        priority=1,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["solaris"])}
    ))
    datastore.create_operation(
        operations_pb2.Operation(
            name="test-operation",
            done=False
        ),
        "test-job"
    )
    datastore.queue_job("test-job")

    datastore.create_job(Job(
        True,
        Digest(hash="other-action", size_bytes=10),
        name="other-job",
        priority=5,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["linux"])}
    ))
    datastore.create_operation(
        operations_pb2.Operation(
            name="other-operation",
            done=False
        ),
        "other-job"
    )
    datastore.queue_job("other-job")

    datastore.create_job(Job(
        True,
        Digest(hash="extra-action", size_bytes=50),
        name="extra-job",
        priority=10,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["linux"])}
    ))
    datastore.create_operation(
        operations_pb2.Operation(
            name="extra-operation",
            done=False
        ),
        "extra-job"
    )
    datastore.queue_job("extra-job")

    datastore.create_job(Job(
        True,
        Digest(hash="chroot-action", size_bytes=50),
        name="chroot-job",
        priority=10,
        stage=1,
        operations=[
        ],
        platform_requirements={
            "OSFamily": set(["aix"]),
            "ChrootDigest": set(["space", "dead-beef"]),
            "ISA": set(["x64", "x32"]),
        }
    ))
    datastore.create_operation(
        operations_pb2.Operation(
            name="chroot-operation",
            done=False
        ),
        "chroot-job"
    )
    datastore.queue_job("chroot-job")


def test_deactivate_monitoring(datastore):
    populate_datastore(datastore)
    assert datastore.is_instrumented

    operation = operations_pb2.Operation(
        name="test-operation-2",
        done=False
    )

    datastore.create_operation(operation, "test-job")
    assert "test-operation-2" in datastore.jobs_by_operation
    assert "test-job" in datastore.operations_by_stage[OperationStage(
        1)]

    datastore.deactivate_monitoring()
    assert datastore.operations_by_stage == {}


def test_delete_job(datastore):
    populate_datastore(datastore)
    datastore.delete_job("test-job")
    assert "test-job" not in datastore.jobs_by_name


def test_assign_lease_for_next_job(datastore):
    populate_datastore(datastore)

    def cb(j):
        lease = j.lease
        if not lease:
            lease = j.create_lease("test-suite", data_store=datastore)
        if lease:
            j.mark_worker_started()
            return [lease]
        return []

    # The highest priority runnable job with requirements matching these
    # capabilities is other-job, which is priority 5 and only requires linux
    leases = datastore.assign_lease_for_next_job(
        {"OSFamily": set(["linux"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    datastore.queue_job("other-job")

    # The highest priority runnable job for these capabilities is still
    # other-job, since priority 5 is more urgent than the priority 20 of
    # example-job. test-job has priority 1, but its requirements are not
    # fulfilled by these capabilities
    leases = datastore.assign_lease_for_next_job(
        {"OSFamily": set(["linux"]), "generic": set(["requirement"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    # The highest priority runnable job for this magical machine which has
    # multiple values for the `os` capability is test-job, since its requirements
    # are fulfilled and it has priority 1, compared with priority 5 for other-job
    leases = datastore.assign_lease_for_next_job(
        {"OSFamily": set(["linux", "solaris"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "test-job"

    # Must have all properties or else job won't get matched with worker
    leases = datastore.assign_lease_for_next_job(
        {"OSFamily": set(["aix"])}, cb)
    assert len(leases) == 0

    # Must match with all ISA properties
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["space", "dead-beef"]),
        "ISA": set(["x64"]),
    }
    leases = datastore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 0

    # Digest doesn't match, should not match with any jobs
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef"]),
        "ISA": set(["x64", "x32"]),
    }
    leases = datastore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 0

    # All fields match, should match with chroot-job
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef", "space"]),
        "ISA": set(["x64", "x32"]),
    }
    leases = datastore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 1
    assert leases[0].id == "chroot-job"

    datastore.queue_job("chroot-job")

    # All fields subset of worker's, should match with chroot-job
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef", "space", "outer"]),
        "ISA": set(["x64", "x32", "x16"]),
    }
    leases = datastore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 1
    assert leases[0].id == "chroot-job"
