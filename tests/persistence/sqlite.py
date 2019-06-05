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

from buildgrid._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from buildgrid._protos.google.longrunning import operations_pb2
from buildgrid.server.job import Job
from buildgrid.server.persistence import DataStore
from buildgrid.server.persistence.sql import models
from buildgrid.server.persistence.sql.impl import SQLDataStore


@pytest.fixture()
def database():
    _, db = tempfile.mkstemp()
    DataStore.backend = SQLDataStore(connection_string="sqlite:///%s" % db, automigrate=True)
    yield
    DataStore.backend = None
    if os.path.exists(db):
        os.remove(db)


def add_test_job(job_name):
    with DataStore.backend.session() as session:
        session.add(models.Job(
            name=job_name,
            action_digest="test-action-digest/144",
            priority=10,
            stage=1
        ))


def populate_database():
    with DataStore.backend.session() as session:
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
                platform_requirements={
                    "os": "solaris"
                }
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
                platform_requirements={
                    "os": "linux"
                }
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
                platform_requirements={
                    "os": "linux",
                    "generic": "requirement"
                }
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
                platform_requirements={
                    "os": "linux"
                }
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
                platform_requirements={
                    "os": "linux"
                }
            )
        ])


def test_rollback(database):
    job_name = "test-job"
    add_test_job(job_name)
    with DataStore.backend.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        job.name = "other-job"
        raise Exception("Forced exception")

    with DataStore.backend.session() as session:
        # This query will only return a result if the rollback was successful and
        # the job name wasn't changed
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None


def test_create_job(database):
    job_name = "test-job"
    job = Job(do_not_cache=False,
              action_digest=models.string_to_digest("test-action-digest/144"),
              priority=10,
              name=job_name)
    DataStore.create_job(job)

    with DataStore.backend.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        assert job.priority == 10
        assert job.action_digest == "test-action-digest/144"
        assert not job.do_not_cache


def test_update_job(database):
    job_name = "test-job"
    add_test_job(job_name)

    DataStore.update_job(job_name, {"priority": 1})

    with DataStore.backend.session() as session:
        job = session.query(models.Job).filter_by(name=job_name).first()
        assert job is not None
        assert job.priority == 1


def test_create_operation(database):
    job_name = "test-job"
    add_test_job(job_name)

    op_name = "test-operation"
    done = False
    operation = operations_pb2.Operation()
    operation.name = op_name
    operation.done = done

    DataStore.create_operation(operation, job_name)

    with DataStore.backend.session() as session:
        op = session.query(models.Operation).filter_by(name=op_name).first()
        assert op is not None
        assert op.job.name == job_name
        assert op.name == op_name
        assert op.done == done


def test_update_operation(database):
    job_name = "test-job"
    add_test_job(job_name)

    op_name = "test-operation"
    done = False
    with DataStore.backend.session() as session:
        session.add(models.Operation(
            name=op_name,
            job_name=job_name,
            done=done
        ))

    DataStore.update_operation(op_name, {"done": True})

    with DataStore.backend.session() as session:
        op = session.query(models.Operation).filter_by(name=op_name).first()
        assert op is not None
        assert op.job.name == job_name
        assert op.name == op_name
        assert op.done


def test_create_lease(database):
    job_name = "test-job"
    add_test_job(job_name)

    state = 0
    lease = bots_pb2.Lease()
    lease.id = job_name
    lease.state = state

    DataStore.create_lease(lease)

    with DataStore.backend.session() as session:
        lease = session.query(models.Lease).filter_by(job_name=job_name).first()
        assert lease is not None
        assert lease.job.name == job_name
        assert lease.state == state


def test_update_lease(database):
    job_name = "test-job"
    add_test_job(job_name)

    state = 0
    with DataStore.backend.session() as session:
        session.add(models.Lease(
            job_name=job_name,
            state=state
        ))

    DataStore.update_lease(job_name, {"state": 1})
    with DataStore.backend.session() as session:
        lease = session.query(models.Lease).filter_by(job_name=job_name).first()
        assert lease is not None
        assert lease.job.name == job_name
        assert lease.state == 1


def test_load_unfinished_jobs(database):
    populate_database()

    jobs = DataStore.load_unfinished_jobs()
    assert jobs
    assert jobs[0].name == "test-job"


def test_get_next_runnable_job(database):
    populate_database()

    # The highest priority runnable job with requirements matching these
    # capabilities is other-job, which is priority 5 and only requires linux
    job = DataStore.get_next_runnable_job({"os": ["linux"]})
    assert job.name == "other-job"

    # The highest priority runnable job for these capabilities is still
    # other-job, since priority 5 is more urgent than the priority 20 of
    # example-job. test-job has priority 1, but its requirements are not
    # fulfilled by these capabilities
    job = DataStore.get_next_runnable_job({"os": ["linux"], "generic": ["requirement"]})
    assert job.name == "other-job"

    # The highest priority runnable job for this magical machine which has
    # multiple values for the `os` capability is test-job, since its requirements
    # are fulfilled and it has priority 1, compared with priority 5 for other-job
    job = DataStore.get_next_runnable_job({"os": ["linux", "solaris"]})
    assert job.name == "test-job"


def test_to_internal_job(database):
    populate_database()

    with DataStore.backend.session() as session:
        job = session.query(models.Job).filter_by(name="finished-job").first()
        internal_job = job.to_internal_job()
    assert internal_job.operation_stage.value == 4

    with DataStore.backend.session() as session:
        job = session.query(models.Job).filter_by(name="cancelled-job").first()
        internal_job = job.to_internal_job()
    assert internal_job.cancelled
    assert internal_job.operation_stage.value == 4
