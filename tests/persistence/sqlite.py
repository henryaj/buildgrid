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
from unittest import mock
from sqlalchemy.pool.impl import StaticPool

from buildgrid._enums import LeaseState, OperationStage
from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.cas.storage import lru_memory_cache
from buildgrid.server.job import Job
from buildgrid.server.persistence.sql import models
from buildgrid.server.persistence.sql.impl import SQLDataStore


@pytest.fixture()
def database():
    storage = lru_memory_cache.LRUMemoryCache(1024 * 1024)
    _, db = tempfile.mkstemp()
    data_store = SQLDataStore(storage, connection_string="sqlite:///%s" % db, automigrate=True)
    try:
        yield data_store
    finally:
        if os.path.exists(db):
            os.remove(db)


def add_test_job(job_name, database):
    with database.session() as session:
        session.add(models.Job(
            name=job_name,
            action_digest="test-action-digest/144",
            priority=10,
            stage=1
        ))


def populate_database(database):
    with database.session() as session:
        session.add_all([
            models.Job(
                name="test-job",
                action_digest="test-action/100",
                priority=1,
                stage=2,
                leases=[models.Lease(
                    status=0,
                    state=2
                )],
                operations=[
                    models.Operation(
                        name="test-operation",
                        done=False
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="solaris"
                    )
                ]
            ),
            models.Job(
                name="other-job",
                action_digest="other-action/10",
                priority=5,
                stage=2,
                leases=[models.Lease(
                    status=0,
                    state=1
                )],
                operations=[
                    models.Operation(
                        name="other-operation",
                        done=False
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="linux"
                    )
                ]
            ),
            models.Job(
                name="extra-job",
                action_digest="extra-action/50",
                priority=20,
                stage=2,
                leases=[models.Lease(
                    status=0,
                    state=1
                )],
                operations=[
                    models.Operation(
                        name="extra-operation",
                        done=False
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="linux"
                    ),
                    models.PlatformRequirement(
                        key="generic",
                        value="requirement"
                    )
                ]
            ),
            models.Job(
                name="cancelled-job",
                action_digest="cancelled-action/35",
                priority=20,
                stage=4,
                cancelled=True,
                queued_timestamp=datetime(2019, 6, 1),
                queued_time_duration=60,
                worker_start_timestamp=datetime(2019, 6, 1, minute=1),
                leases=[models.Lease(
                    status=0,
                    state=5
                )],
                operations=[
                    models.Operation(
                        name="cancelled-operation",
                        done=True
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="linux"
                    )
                ]
            ),
            models.Job(
                name="finished-job",
                action_digest="finished-action/35",
                priority=20,
                stage=4,
                queued_timestamp=datetime(2019, 6, 1),
                queued_time_duration=10,
                worker_start_timestamp=datetime(2019, 6, 1, second=10),
                worker_completed_timestamp=datetime(2019, 6, 1, minute=1),
                leases=[models.Lease(
                    status=0,
                    state=4
                )],
                operations=[
                    models.Operation(
                        name="finished-operation",
                        done=True
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="linux"
                    )
                ]
            ),
            models.Job(
                name="platform-job",
                action_digest="platform-action/10",
                priority=5,
                stage=2,
                leases=[models.Lease(
                    status=0,
                    state=1
                )],
                operations=[
                    models.Operation(
                        name="platform-operation",
                        done=False
                    )
                ],
                platform_requirements=[
                    models.PlatformRequirement(
                        key="os",
                        value="aix"
                    ),
                    models.PlatformRequirement(
                        key="generic",
                        value="requirement"
                    ),
                    models.PlatformRequirement(
                        key="generic",
                        value="requirement2"
                    )
                ]
            )
        ])


@pytest.mark.parametrize("conn_str", ["sqlite:///file:memdb1?option=value&cache=shared&mode=memory",
                                      "sqlite:///file:memdb1?mode=memory&cache=shared",
                                      "sqlite:///file:memdb1?cache=shared&mode=memory",
                                      "sqlite:///file::memory:?cache=shared",
                                      "sqlite:///file::memory:",
                                      "sqlite:///:memory:",
                                      "sqlite:///",
                                      "sqlite://"])
def test_is_sqlite_inmemory_connection_string(conn_str):
    with pytest.raises(ValueError):
        # Should raise ValueError when trying to instantiate
        database = SQLDataStore(None, connection_string=conn_str)


@pytest.mark.parametrize("conn_str", ["sqlite:///../../myfile.db",
                                      "sqlite:///./myfile.db",
                                      "sqlite:////myfile.db"])
def test_file_based_sqlite_db(conn_str):
    # Those should be OK and not raise anything during instantiation
    with mock.patch('buildgrid.server.persistence.sql.impl.create_engine') as create_engine:
        database = SQLDataStore(None, connection_string=conn_str)
        assert create_engine.call_count == 1
        call_args, call_kwargs = create_engine.call_args

        # Let's make sure that sqlite will behave OK with multiple threads
        assert call_kwargs['poolclass'] == StaticPool
        assert isinstance(call_kwargs['connect_args']['check_same_thread'], bool)
        assert not call_kwargs['connect_args']['check_same_thread']


def test_rollback(database):
    job_name = "test-job"
    add_test_job(job_name, database)
    with database.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        job.name = "other-job"
        raise Exception("Forced exception")

    with database.session() as session:
        # This query will only return a result if the rollback was successful and
        # the job name wasn't changed
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None


def test_get_job_by_action(database):
    populate_database(database)
    job = database.get_job_by_action("notarealjob")
    assert job is None

    # Ensure that get_job_by_action doesn't get completed jobs.
    # Actions aren't unique in the job history, so we only care
    # about the one that is currently incomplete (if any).
    job = database.get_job_by_action(models.string_to_digest("finished-action/35"))
    assert job is None

    job = database.get_job_by_action(models.string_to_digest("extra-action/50"))
    assert job.name == "extra-job"
    assert job.priority == 20


def test_get_job_by_name(database):
    populate_database(database)
    job = database.get_job_by_name("notarealjob")
    assert job is None

    job = database.get_job_by_name("extra-job")
    assert job.name == "extra-job"
    assert job.priority == 20


def test_get_job_by_operation(database):
    populate_database(database)
    job = database.get_job_by_operation("notarealjob")
    assert job is None

    job = database.get_job_by_operation("extra-operation")
    assert job.name == "extra-job"
    assert job.priority == 20


def test_get_all_jobs(database):
    populate_database(database)
    jobs = database.get_all_jobs()
    assert len(jobs) == 4


def test_create_job(database):
    job_name = "test-job"
    job = Job(database,
              do_not_cache=False,
              action_digest=models.string_to_digest("test-action-digest/144"),
              priority=10,
              name=job_name)
    database.create_job(job)

    with database.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        assert job.priority == 10
        assert job.action_digest == "test-action-digest/144"
        assert not job.do_not_cache


def test_update_job(database):
    job_name = "test-job"
    add_test_job(job_name, database)

    database.update_job(job_name, {"priority": 1})

    with database.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        assert job.priority == 1


def test_delete_job(database):
    populate_database(database)
    job = database.get_job_by_name("test-job")
    database.store_response(job)
    assert "test-job" in database.response_cache

    database.delete_job("test-job")
    assert "test-job" not in database.response_cache


def test_store_response(database):
    populate_database(database)
    job = database.get_job_by_name("test-job")
    database.store_response(job)

    updated = database.get_job_by_name("test-job")
    assert updated.execute_response is not None
    assert "test-job" in database.response_cache
    assert database.response_cache["test-job"] is not None


def test_get_operations_by_stage(database):
    populate_database(database)
    operations = database.get_operations_by_stage(OperationStage(4))
    assert len(operations) == 2


def test_get_all_operations(database):
    populate_database(database)
    operations = database.get_all_operations()
    assert len(operations) == 6


def test_create_operation(database):
    job_name = "test-job"
    add_test_job(job_name, database)

    op_name = "test-operation"
    done = False
    operation = operations_pb2.Operation()
    operation.name = op_name
    operation.done = done

    database.create_operation(operation, job_name)

    with database.session() as session:
        op = session.query(models.Operation).filter_by(name=op_name).first()
        assert op is not None
        assert op.job.name == job_name
        assert op.name == op_name
        assert op.done == done


def test_update_operation(database):
    job_name = "test-job"
    add_test_job(job_name, database)

    op_name = "test-operation"
    done = False
    with database.session() as session:
        session.add(models.Operation(
            name=op_name,
            job_name=job_name,
            done=done
        ))

    database.update_operation(op_name, {"done": True})

    with database.session() as session:
        op = session.query(models.Operation).filter_by(name=op_name).first()
        assert op is not None
        assert op.job.name == job_name
        assert op.name == op_name
        assert op.done


def test_get_leases_by_state(database):
    populate_database(database)
    leases = database.get_leases_by_state(LeaseState(1))
    assert len(leases) == 3


def test_create_lease(database):
    job_name = "test-job"
    add_test_job(job_name, database)

    state = 0
    lease = bots_pb2.Lease()
    lease.id = job_name
    lease.state = state

    database.create_lease(lease)

    with database.session() as session:
        lease = session.query(models.Lease).filter_by(job_name=job_name).first()
        assert lease is not None
        assert lease.job.name == job_name
        assert lease.state == state


def test_update_lease(database):
    job_name = "test-job"
    add_test_job(job_name, database)

    state = 0
    with database.session() as session:
        session.add(models.Lease(
            job_name=job_name,
            state=state
        ))

    database.update_lease(job_name, {"state": 1})
    with database.session() as session:
        lease = session.query(models.Lease).filter_by(job_name=job_name).first()
        assert lease is not None
        assert lease.job.name == job_name
        assert lease.state == 1


def test_load_unfinished_jobs(database):
    populate_database(database)

    jobs = database.load_unfinished_jobs()
    assert jobs
    assert jobs[0].name == "test-job"


def test_assign_lease_for_next_job(database):
    populate_database(database)

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
    leases = database.assign_lease_for_next_job({"os": ["linux"]}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    database.queue_job("other-job")

    # The highest priority runnable job for these capabilities is still
    # other-job, since priority 5 is more urgent than the priority 20 of
    # example-job. test-job has priority 1, but its requirements are not
    # fulfilled by these capabilities
    leases = database.assign_lease_for_next_job({"os": ["linux"], "generic": ["requirement"]}, cb)
    assert len(leases) == 1
    assert leases[0].id == "other-job"

    # The highest priority runnable job for this magical machine which has
    # multiple values for the `os` capability is test-job, since its requirements
    # are fulfilled and it has priority 1, compared with priority 5 for other-job
    leases = database.assign_lease_for_next_job({"os": ["linux", "solaris"]}, cb)
    assert len(leases) == 1
    assert leases[0].id == "test-job"

    # Shouldn't match with platform-job, worker only has one of the two requirements
    leases = database.assign_lease_for_next_job({"os": ["aix"], "generic": ["requirement"]}, cb)
    assert len(leases) == 0

    # Should match with platform-job, has exact specification needed
    leases = database.assign_lease_for_next_job({"os": ["aix"], "generic": ["requirement", "requirement2"]}, cb)
    assert len(leases) == 1
    assert leases[0].id == "platform-job"

    database.queue_job("platform-job")

    # Should match with platform-job, worker has superset of specifications
    specifications = {
        "os": ["aix", "andriod"],
        "generic": ["requirement", "requirement2", "requirement3"]
    }
    leases = database.assign_lease_for_next_job(specifications, cb)
    assert len(leases) == 1
    assert leases[0].id == "platform-job"


def test_to_internal_job(database):
    populate_database(database)

    with database.session() as session:
        job = session.query(models.Job).filter_by(name="finished-job").first()
        internal_job = job.to_internal_job(database)
    assert internal_job.operation_stage.value == 4

    with database.session() as session:
        job = session.query(models.Job).filter_by(name="cancelled-job").first()
        internal_job = job.to_internal_job(database)
    assert internal_job.cancelled
    assert internal_job.operation_stage.value == 4
