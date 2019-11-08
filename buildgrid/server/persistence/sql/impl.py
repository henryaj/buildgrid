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
from tempfile import NamedTemporaryFile

from alembic import command
from alembic.config import Config
from sqlalchemy import and_, create_engine, event, or_
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from ...._enums import OperationStage
from ....settings import MAX_JOB_BLOCK_TIME
from ..interface import DataStoreInterface
from .models import digest_to_string, Job, Lease, Operation, PlatformRequirement


Session = sessionmaker()


def sqlite_on_connect(conn, record):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")


class SQLDataStore(DataStoreInterface):

    def __init__(self, storage, *, connection_string=None, automigrate=False,
                 **kwargs):
        self.__logger = logging.getLogger(__name__)
        self.__logger.info("Creating SQL data store interface with: "
                           "automigrate=[%s], kwargs=[%s]",
                           automigrate, kwargs)

        self.storage = storage
        self.response_cache = {}

        # Set-up temporary SQLite Database when connection string is not specified
        if not connection_string:
            tmpdbfile = NamedTemporaryFile(prefix='bgd-', suffix='.db')
            self._tmpdbfile = tmpdbfile  # Make sure to keep this tempfile for the lifetime of this object
            self.__logger.warn("No connection string specified for the DataStore, "
                               "will use SQLite with tempfile: [%s]" % tmpdbfile.name)
            automigrate = True  # since this is a temporary database, we always need to create it
            connection_string = "sqlite:///{}".format(tmpdbfile.name)

        self._create_sqlalchemy_engine(connection_string, automigrate, **kwargs)

    def _create_sqlalchemy_engine(self, connection_string, automigrate, **kwargs):
        self.automigrate = automigrate

        # Disallow sqlite in-memory because multi-threaded access to it is
        # complex and potentially problematic at best
        # ref: https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
        if self._is_sqlite_inmemory_connection_string(connection_string):
            raise ValueError("Cannot use SQLite in-memory with BuildGrid (connection_string=[%s]). "
                             "Use a file or leave the connection_string empty for a tempfile." %
                             connection_string)

        # Only pass the (known) kwargs that have been explicitly set by the user
        available_options = set(['pool_size', 'max_overflow', 'pool_timeout', 'connect_args'])
        kwargs_keys = set(kwargs.keys())
        if not kwargs_keys.issubset(available_options):
            unknown_options = kwargs_keys - available_options
            raise TypeError("Unknown keyword arguments: [%s]" % unknown_options)

        self.__logger.debug("SQLAlchemy additional kwargs: [%s]", kwargs)

        self.engine = create_engine(connection_string, echo=False, **kwargs)
        Session.configure(bind=self.engine)

        if self.engine.dialect.name == "sqlite":
            event.listen(self.engine, "connect", sqlite_on_connect)

        if self.automigrate:
            self._create_or_migrate_db(connection_string)

    def _is_sqlite_connection_string(self, connection_string):
        if connection_string:
            return connection_string.startswith("sqlite")
        return False

    def _is_sqlite_inmemory_connection_string(self, full_connection_string):
        if self._is_sqlite_connection_string(full_connection_string):
            # Valid connection_strings for in-memory SQLite which we don't support could look like:
            # "sqlite:///file:memdb1?option=value&cache=shared&mode=memory",
            # "sqlite:///file:memdb1?mode=memory&cache=shared",
            # "sqlite:///file:memdb1?cache=shared&mode=memory",
            # "sqlite:///file::memory:?cache=shared",
            # "sqlite:///file::memory:",
            # "sqlite:///:memory:",
            # "sqlite:///",
            # "sqlite://"
            # ref: https://www.sqlite.org/inmemorydb.html
            # Note that a user can also specify drivers, so prefix could become 'sqlite+driver:///'
            connection_string = full_connection_string

            uri_split_index = connection_string.find("?")
            if uri_split_index != -1:
                connection_string = connection_string[0:uri_split_index]

            if connection_string.endswith((":memory:", ":///", "://")):
                return True
            elif uri_split_index != -1:
                opts = full_connection_string[uri_split_index + 1:].split("&")
                if "mode=memory" in opts:
                    return True

        return False

    def __repr__(self):
        return "SQL data store interface for `%s`" % repr(self.engine.url)

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

        with self.engine.begin() as connection:
            config.attributes['connection'] = connection
            command.upgrade(config, "head")

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
            return job.to_internal_job(self)

    def get_job_by_name(self, name):
        with self.session() as session:
            job = self._get_job(name, session)
            return job.to_internal_job(self)

    def get_job_by_operation(self, operation_name):
        with self.session() as session:
            operation = self._get_operation(operation_name, session)
            job = operation.job
            return job.to_internal_job(self)

    def get_all_jobs(self):
        with self.session() as session:
            jobs = session.query(Job).filter(Job.stage != OperationStage.COMPLETED.value)
            return [j.to_internal_job(self) for j in jobs]

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
            job = job.to_internal_job(self)
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
            return [j.to_internal_job(self) for j in jobs.all()]

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
                internal_job = job.to_internal_job(self)
                leases = callback(internal_job)
                if leases:
                    job.assigned = True
                    job.worker_start_timestamp = internal_job.worker_start_timestamp.ToDatetime()
                for lease in leases:
                    self._create_lease(lease, session, job=internal_job)
                return leases
            return []
