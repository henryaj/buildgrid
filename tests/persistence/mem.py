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
from buildgrid.server.persistence import DataStore
from buildgrid.server.persistence.mem.impl import MemoryDataStore


@pytest.fixture()
def datastore():
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    DataStore.backend = MemoryDataStore(storage)
    DataStore.activate_monitoring()
    yield
    DataStore.backend = None


def populate_datastore():
    DataStore.create_job(Job(
        True,
        Digest(hash="test-action", size_bytes=100),
        name="test-job",
        priority=1,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["solaris"])}
    ))
    DataStore.create_operation(
        operations_pb2.Operation(
            name="test-operation",
            done=False
        ),
        "test-job"
    )
    DataStore.queue_job("test-job")

    DataStore.create_job(Job(
        True,
        Digest(hash="other-action", size_bytes=10),
        name="other-job",
        priority=5,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["linux"])}
    ))
    DataStore.create_operation(
        operations_pb2.Operation(
            name="other-operation",
            done=False
        ),
        "other-job"
    )
    DataStore.queue_job("other-job")

    DataStore.create_job(Job(
        True,
        Digest(hash="extra-action", size_bytes=50),
        name="extra-job",
        priority=10,
        stage=1,
        operations=[
        ],
        platform_requirements={"OSFamily": set(["linux"])}
    ))
    DataStore.create_operation(
        operations_pb2.Operation(
            name="extra-operation",
            done=False
        ),
        "extra-job"
    )
    DataStore.queue_job("extra-job")

    DataStore.create_job(Job(
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
    DataStore.create_operation(
        operations_pb2.Operation(
            name="chroot-operation",
            done=False
        ),
        "chroot-job"
    )
    DataStore.queue_job("chroot-job")


def test_deactivate_monitoring(datastore):
    populate_datastore()
    assert DataStore.backend.is_instrumented

    operation = operations_pb2.Operation(
        name="test-operation-2",
        done=False
    )

    DataStore.create_operation(operation, "test-job")
    assert "test-operation-2" in DataStore.backend.jobs_by_operation
    assert "test-job" in DataStore.backend.operations_by_stage[OperationStage(
        1)]

    DataStore.deactivate_monitoring()
    assert DataStore.backend.operations_by_stage == {}


def test_delete_job(datastore):
    populate_datastore()
    DataStore.delete_job("test-job")
    assert "test-job" not in DataStore.backend.jobs_by_name


def test_assign_lease_for_next_job(datastore):
    populate_datastore()

    def cb(j):
        lease = j.lease
        if not lease:
            lease = j.create_lease("test-suite")
        if lease:
            j.mark_worker_started()
            return [lease]
        return []

    # The highest priority runnable job with requirements matching these
    # capabilities is other-job, which is priority 5 and only requires linux
    leases = DataStore.assign_lease_for_next_job(
        {"OSFamily": set(["linux"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    DataStore.queue_job("other-job")

    # The highest priority runnable job for these capabilities is still
    # other-job, since priority 5 is more urgent than the priority 20 of
    # example-job. test-job has priority 1, but its requirements are not
    # fulfilled by these capabilities
    leases = DataStore.assign_lease_for_next_job(
        {"OSFamily": set(["linux"]), "generic": set(["requirement"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    # The highest priority runnable job for this magical machine which has
    # multiple values for the `os` capability is test-job, since its requirements
    # are fulfilled and it has priority 1, compared with priority 5 for other-job
    leases = DataStore.assign_lease_for_next_job(
        {"OSFamily": set(["linux", "solaris"])}, cb)
    assert len(leases) == 1
    assert leases[0].id == "test-job"

    # Must have all properties or else job won't get matched with worker
    leases = DataStore.assign_lease_for_next_job(
        {"OSFamily": set(["aix"])}, cb)
    assert len(leases) == 0

    # Must match with all ISA properties
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["space", "dead-beef"]),
        "ISA": set(["x64"]),
    }
    leases = DataStore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 0

    # Digest doesn't match, should not match with any jobs
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef"]),
        "ISA": set(["x64", "x32"]),
    }
    leases = DataStore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 0

    # All fields match, should match with chroot-job
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef", "space"]),
        "ISA": set(["x64", "x32"]),
    }
    leases = DataStore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 1
    assert leases[0].id == "chroot-job"

    DataStore.queue_job("chroot-job")

    # All fields subset of worker's, should match with chroot-job
    worker_capabilities = {
        "OSFamily": set(["aix"]),
        "ChrootDigest": set(["dead-beef", "space", "outer"]),
        "ISA": set(["x64", "x32", "x16"]),
    }
    leases = DataStore.assign_lease_for_next_job(worker_capabilities, cb)
    assert len(leases) == 1
    assert leases[0].id == "chroot-job"
