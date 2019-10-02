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


from contextlib import contextmanager
import logging
import os
import time

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, create_engine, or_
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.exc import OperationalError

from ...._enums import OperationStage
from ....settings import MAX_JOB_BLOCK_TIME
from ..interface import DataStoreInterface
from .models import digest_to_string, Job, Lease, Operation, PlatformRequirement


Session = sessionmaker()


class SQLDataStore(DataStoreInterface):

    def __init__(self, storage, *, connection_string="sqlite:///", automigrate=False,
                 retry_limit=10, **kwargs):
        self.__logger = logging.getLogger(__name__)
        self.__logger.info("Using SQL data store interface with: "
                           "automigrate=[%s], retry_limit=[%s], kwargs=[%s]",
                           automigrate, retry_limit, kwargs)

        self.storage = storage
        self.response_cache = {}

        self.automigrate = automigrate
        self.retry_limit = retry_limit

        # Only pass the (known) kwargs that have been explicitly set by the user
        available_options = set(['pool_size', 'max_overflow', 'pool_timeout'])
        kwargs_keys = set(kwargs.keys())
        if not kwargs_keys.issubset(available_options):
            unknown_options = kwargs_keys - available_options
            raise TypeError("Unknown keyword arguments: [%s]" % unknown_options)

        self.__logger.debug("SQLAlchemy additional kwargs: [%s]", kwargs)

        self.engine = create_engine(connection_string, echo=False, **kwargs)
        Session.configure(bind=self.engine)

        if self.automigrate:
            self._create_or_migrate_db(connection_string)

    def activate_monitoring(self):
        # Don't do anything. This function needs to exist but there's no
        # need to actually toggle monitoring in this implementation.
        pass

    def deactivate_monitoring(self):
        # Don't do anything. This function needs to exist but there's no
        # need to actually toggle monitoring in this implementation.
        pass

    def _create_or_migrate_db(self, connection_string):
        self.__logger.warn("Will attempt migration to latest version if needed.")

        config = Config()
        config.set_main_option("script_location", os.path.join(os.path.dirname(__file__), "alembic"))
        config.set_main_option("sqlalchemy.url", connection_string)

        retries = 0
        while retries < self.retry_limit:
            try:
                command.upgrade(config, "head")
                break
            except OperationalError:
                retries += 1
                time.sleep(retries * 5)

    @contextmanager
    def session(self):
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            self.__logger.error("Error in database session.", exc_info=True)
        finally:
            session.close()

    def _get_job(self, job_name, session, with_for_update=False):
        jobs = session.query(Job)
        if with_for_update:
            jobs = jobs.with_for_update()
        jobs = jobs.filter_by(name=job_name)
        return jobs.first()

    def get_job_by_action(self, action_digest):
        with self.session() as session:
            jobs = session.query(Job).filter_by(action_digest=digest_to_string(action_digest))
            jobs = jobs.filter(Job.stage != OperationStage.COMPLETED.value)
            job = jobs.first()
            if not job:
                return None
            return job.to_internal_job(self.storage, self.response_cache)

    def get_job_by_name(self, name):
        with self.session() as session:
            job = self._get_job(name, session)
            return job.to_internal_job(self.storage, self.response_cache)

    def get_job_by_operation(self, operation_name):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            job = operation.job
            return job.to_internal_job(self.storage, self.response_cache)

    def get_all_jobs(self):
        with self.session() as session:
            jobs = session.query(Job).filter(Job.stage != OperationStage.COMPLETED.value)
            return [j.to_internal_job(self.storage, self.response_cache) for j in jobs]

    def create_job(self, job):
        with self.session() as session:
            if self._get_job(job.name, session) is None:
                requirements = [
                    PlatformRequirement(key=k, value=v)
                    for k, values in job.platform_requirements.items()
                    for v in values
                ]
                session.add(Job(
                    name=job.name,
                    action_digest=digest_to_string(job.action_digest),
                    do_not_cache=job.do_not_cache,
                    priority=job.priority,
                    operations=[],
                    platform_requirements=requirements,
                    stage=job.operation_stage.value,
                    queued_timestamp=job.queued_timestamp.ToDatetime(),
                    queued_time_duration=job.queued_time_duration.seconds,
                    worker_start_timestamp=job.worker_start_timestamp.ToDatetime(),
                    worker_completed_timestamp=job.worker_completed_timestamp.ToDatetime()
                ))
                self.response_cache[job.name] = job.execute_response

    def queue_job(self, job_name):
        with self.session() as session:
            job = self._get_job(job_name, session, with_for_update=True)
            job.assigned = False

    def update_job(self, job_name, changes):
        if "result" in changes:
            changes["result"] = digest_to_string(changes["result"])
        if "action_digest" in changes:
            changes["action_digest"] = digest_to_string(changes["action_digest"])

        with self.session() as session:
            job = self._get_job(job_name, session)
            job.update(changes)

    def delete_job(self, job_name):
        if job_name in self.response_cache:
            del self.response_cache[job_name]

    def store_response(self, job):
        digest = self.storage.put_message(job.execute_response)
        self.update_job(job.name, {"result": digest})
        self.response_cache[job.name] = job.execute_response

    def _get_operation(self, operation_name, session):
        operations = session.query(Operation).filter_by(name=operation_name)
        return operations.first()

    def get_operations_by_stage(self, operation_stage):
        with self.session() as session:
            operations = session.query(Operation)
            operations = operations.filter(Operation.job.has(stage=operation_stage.value))
            operations = operations.all()
            # Return a set of job names here for now, to match the `MemoryDataStore`
            # implementation's behaviour
            return set(op.job.name for op in operations)

    def get_all_operations(self):
        with self.session() as session:
            operations = session.query(Operation)
            return [op.name for op in operations]

    def create_operation(self, operation, job_name):
        with self.session() as session:
            session.add(Operation(
                name=operation.name,
                job_name=job_name,
                done=operation.done
            ))

    def update_operation(self, operation_name, changes):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            operation.update(changes)

    def delete_operation(self, operation_name):
        # Don't do anything. This function needs to exist but there's no
        # need to actually delete operations in this implementation.
        pass

    def get_leases_by_state(self, lease_state):
        with self.session() as session:
            leases = session.query(Lease).filter_by(state=lease_state.value)
            leases = leases.all()
            # `lease.job_name` is the same as `lease.id` for a Lease protobuf
            return set(lease.job_name for lease in leases)

    def _create_lease(self, lease, session, job=None):
        if job is None:
            job = self._get_job(lease.id, session)
            job = job.to_internal_job(self.storage, self.response_cache)
        session.add(Lease(
            job_name=lease.id,
            state=lease.state,
            status=None,
            worker_name=job.worker_name
        ))

    def create_lease(self, lease):
        with self.session() as session:
            self._create_lease(lease, session)

    def update_lease(self, job_name, changes):
        with self.session() as session:
            job = self._get_job(job_name, session)
            lease = job.active_leases[0]
            lease.update(changes)

    def load_unfinished_jobs(self):
        with self.session() as session:
            jobs = session.query(Job)
            jobs = jobs.filter(Job.stage != OperationStage.COMPLETED.value)
            jobs = jobs.order_by(Job.priority)
            return [j.to_internal_job(self.storage, self.response_cache) for j in jobs.all()]

    def assign_lease_for_next_job(self, capabilities, callback, timeout=None):
        """Return a list of leases for the highest priority jobs that can be run by a worker.

        NOTE: Currently the list only ever has one or zero leases.

        Query the jobs table to find queued jobs which match the capabilities of
        a given worker, and return the one with the highest priority. Takes a
        dictionary of worker capabilities to compare with job requirements.

        :param capabilities: Dictionary of worker capabilities to compare
            with job requirements when finding a job.
        :type capabilities: dict
        :param callback: Function to run on the next runnable job, should return
            a list of leases.
        :type callback: function
        :param timeout: time to wait for new jobs, caps if longer
            than MAX_JOB_BLOCK_TIME.
        :type timeout: int
        :returns: List of leases

        """
        if not timeout:
            return self._assign_job_leases(capabilities, callback)

        # Cap the timeout if it's larger than MAX_JOB_BLOCK_TIME
        if timeout:
            timeout = min(timeout, MAX_JOB_BLOCK_TIME)

        start = time.time()
        while time.time() + 1 - start < timeout:
            leases = self._assign_job_leases(capabilities, callback)
            if leases:
                return leases
            time.sleep(0.5)

    def _assign_job_leases(self, capabilities, callback):
        with self.session() as session:
            jobs = session.query(Job).with_for_update()
            jobs = jobs.filter(Job.stage == OperationStage.QUEUED.value)
            jobs = jobs.filter(Job.assigned != True)  # noqa

            # Filter to find just jobs that either don't have requirements, or
            # jobs with requirement keys that are a subset of the worker's
            # advertised capabilities keys
            jobs = jobs.filter(or_(
                ~Job.platform_requirements.any(),
                and_(
                    Job.platform_requirements.any(
                        PlatformRequirement.key.in_(capabilities.keys())
                    ),
                    ~Job.platform_requirements.any(
                        ~PlatformRequirement.key.in_(capabilities.keys())
                    )
                )
            ))

            # For each advertised worker capability, filter to find jobs which
            # either don't require that capability or can be satisfied by the
            # capability
            for key, value in capabilities.items():
                jobs = jobs.filter(or_(
                    ~Job.platform_requirements.any(),
                    ~Job.platform_requirements.any(PlatformRequirement.key == key),
                    and_(
                        ~Job.platform_requirements.any(and_(
                            PlatformRequirement.key == key,
                            ~PlatformRequirement.value.in_(value)
                        )),
                        Job.platform_requirements.any(and_(
                            PlatformRequirement.key == key,
                            PlatformRequirement.value.in_(value)
                        ))
                    )
                ))

            jobs = jobs.order_by(Job.priority)
            job = jobs.first()
            if job:
                internal_job = job.to_internal_job(self.storage, self.response_cache)
                leases = callback(internal_job)
                if leases:
                    job.assigned = True
                    job.worker_start_timestamp = internal_job.worker_start_timestamp.ToDatetime()
                for lease in leases:
                    self._create_lease(lease, session, job=internal_job)
                return leases
            return []
