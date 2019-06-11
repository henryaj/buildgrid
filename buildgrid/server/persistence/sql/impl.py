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
from ..interface import DataStoreInterface
from .models import digest_to_string, Job, Lease, Operation, PlatformRequirement


Session = sessionmaker()


class SQLDataStore(DataStoreInterface):

    def __init__(self, *, connection_string="sqlite:///", automigrate=False, retry_limit=10):
        self.__logger = logging.getLogger(__name__)

        self.automigrate = automigrate
        self.retry_limit = retry_limit
        self.engine = create_engine(connection_string, echo=False)
        Session.configure(bind=self.engine)

        cfg = Config()
        cfg.set_main_option("script_location",
                            os.path.join(os.path.dirname(__file__), "alembic"))
        cfg.set_main_option("sqlalchemy.url", connection_string)
        self._create_db(cfg)

    def _create_db(self, config):
        retries = 0
        while retries < self.retry_limit:
            try:
                if self.automigrate:
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

    def _get_job(self, job_name, session):
        jobs = session.query(Job).filter_by(name=job_name)
        return jobs.first()

    def create_job(self, job):
        with self.session() as session:
            if self._get_job(job.name, session) is None:
                session.add(Job(
                    name=job.name,
                    action_digest=digest_to_string(job.action_digest),
                    do_not_cache=job.do_not_cache,
                    priority=job.priority,
                    operations=[],
                    platform_requirements=job.platform_requirements,
                    stage=job.operation_stage.value,
                    queued_timestamp=job.queued_timestamp.ToDatetime(),
                    queued_time_duration=job.queued_time_duration.seconds,
                    worker_start_timestamp=job.worker_start_timestamp.ToDatetime(),
                    worker_completed_timestamp=job.worker_completed_timestamp.ToDatetime()
                ))

    def update_job(self, job_name, changes):
        with self.session() as session:
            job = self._get_job(job_name, session)
            job.update(changes)

    def _get_operation(self, operation_name, session):
        operations = session.query(Operation).filter_by(name=operation_name)
        return operations.first()

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

    def create_lease(self, lease):
        with self.session() as session:
            session.add(Lease(
                job_name=lease.id,
                state=lease.state,
                status=None
            ))

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
            return [j.to_internal_job() for j in jobs.all()]

    # NOTE(SotK): This isn't actually used anywhere yet
    def get_next_runnable_job(self, capabilities):
        """Return the highest priority job that can be run by a worker.

        Query the jobs table to find queued jobs which match the capabilities of
        a given worker, and return the one with the highest priority. Takes a
        dictionary of worker capabilities to compare with job requirements.

        :param capabilities: Dictionary of worker capabilities to compare
            with job requirements when finding a job.
        :type capabilities: dict
        :returns: A job

        """
        with self.session() as session:
            jobs = session.query(Job).with_for_update()
            jobs = jobs.filter(Job.stage == OperationStage.QUEUED.value)

            # Filter to find just jobs that either don't have requirements, or
            # jobs with requirement keys that are a subset of the worker's
            # advertised capabilities keys
            jobs = jobs.filter(or_(
                ~Job.reqs.any(),
                and_(
                    Job.reqs.any(PlatformRequirement.key.in_(capabilities.keys())),
                    ~Job.reqs.any(~PlatformRequirement.key.in_(capabilities.keys()))
                )
            ))

            # For each advertised worker capability, filter to find jobs which
            # either don't require that capability or can be satisfied by the
            # capability
            for key, value in capabilities.items():
                jobs = jobs.filter(or_(
                    ~Job.reqs.any(),
                    ~Job.reqs.any(PlatformRequirement.key == key),
                    Job.reqs.any(and_(
                        PlatformRequirement.key == key,
                        PlatformRequirement.value.in_(value)
                    ))
                ))

            jobs = jobs.order_by(Job.priority)
            job = jobs.first()
            return job.to_internal_job()
