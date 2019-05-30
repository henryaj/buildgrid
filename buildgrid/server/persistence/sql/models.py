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


from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from ...._enums import LeaseState
from ...._protos.build.bazel.remote.execution.v2.remote_execution_pb2 import Digest, ExecuteOperationMetadata
from ...._protos.google.devtools.remoteworkers.v1test2 import bots_pb2
from ...._protos.google.longrunning import operations_pb2
from ... import job


class Base:

    """Base class which implements functionality relevant to all models."""

    def update(self, changes):
        for key, val in changes.items():
            setattr(self, key, val)


Base = declarative_base(cls=Base)


class PlatformRequirement(Base):
    __tablename__ = 'platform_requirements'

    id = Column(Integer, primary_key=True)
    job_name = Column(String, ForeignKey('jobs.name'), nullable=False)
    key = Column(String, nullable=False)
    value = Column(String, nullable=False)


Index('ix_platform_requirements_key_value', PlatformRequirement.key, PlatformRequirement.value)


class Job(Base):
    __tablename__ = 'jobs'

    name = Column(String, primary_key=True)
    action_digest = Column(String, index=True, nullable=False)
    priority = Column(Integer, default=1, index=True, nullable=False)
    stage = Column(Integer, default=0, index=True, nullable=False)
    do_not_cache = Column(Boolean, default=False, nullable=False)
    cancelled = Column(Boolean, default=False, nullable=False)
    queued_timestamp = Column(DateTime)
    queued_time_duration = Column(Integer)
    worker_start_timestamp = Column(DateTime)
    worker_completed_timestamp = Column(DateTime)

    leases = relationship('Lease', backref='job')
    active_states = [
        LeaseState.UNSPECIFIED.value,
        LeaseState.PENDING.value,
        LeaseState.ACTIVE.value
    ]
    active_leases = relationship(
        'Lease',
        primaryjoin='and_(Lease.job_name==Job.name, Lease.state.in_(%s))' % active_states
    )

    operations = relationship('Operation', backref='job')

    reqs = relationship('PlatformRequirement', backref='job',
                        collection_class=attribute_mapped_collection('key'))
    platform_requirements = association_proxy(
        'reqs', 'value',
        creator=lambda k, v: PlatformRequirement(key=k, value=v)
    )

    def to_internal_job(self):
        # There should never be more than one active lease for a job. If we
        # have more than one for some reason, just take the first one.
        # TODO(SotK): Log some information here if there are multiple active
        # (ie. not completed or cancelled) leases.
        lease = self.active_leases[0].to_protobuf() if self.active_leases else None
        q_timestamp = Timestamp()
        if self.queued_timestamp:
            q_timestamp.FromDatetime(self.queued_timestamp)
        q_time_duration = Duration()
        if self.queued_time_duration:
            q_time_duration.FromSeconds(self.queued_time_duration)
        ws_timestamp = Timestamp()
        if self.worker_start_timestamp:
            ws_timestamp.FromDatetime(self.worker_start_timestamp)
        wc_timestamp = Timestamp()
        if self.worker_completed_timestamp:
            wc_timestamp.FromDatetime(self.worker_completed_timestamp)
        return job.Job(
            self.do_not_cache,
            string_to_digest(self.action_digest),
            platform_requirements=self.platform_requirements,
            priority=self.priority,
            name=self.name,
            operations=[op.to_protobuf() for op in self.operations],
            lease=lease,
            stage=self.stage,
            cancelled=self.cancelled,
            queued_timestamp=q_timestamp,
            queued_time_duration=q_time_duration,
            worker_start_timestamp=ws_timestamp,
            worker_completed_timestamp=wc_timestamp
        )


class Lease(Base):
    __tablename__ = 'leases'

    id = Column(Integer, primary_key=True)
    job_name = Column(String, ForeignKey('jobs.name'), index=True, nullable=False)
    status = Column(Integer)
    state = Column(Integer, nullable=False)
    worker_name = Column(String, index=True, default=None)

    def to_protobuf(self):
        lease = bots_pb2.Lease()
        lease.id = self.job_name
        lease.payload.Pack(string_to_digest(self.job.action_digest))
        lease.state = self.state
        if self.status is not None:
            lease.status.code = self.status
        return lease


class Operation(Base):
    __tablename__ = 'operations'

    name = Column(String, primary_key=True)
    job_name = Column(String, ForeignKey('jobs.name'), index=True, nullable=False)
    done = Column(Boolean, default=False, nullable=False)

    def to_protobuf(self):
        operation = operations_pb2.Operation()
        operation.name = self.name
        operation.done = self.done
        operation.metadata.Pack(ExecuteOperationMetadata(
            stage=self.job.stage,
            action_digest=string_to_digest(self.job.action_digest)))
        return operation


def digest_to_string(digest):
    return '{}/{}'.format(digest.hash, digest.size_bytes)


def string_to_digest(string):
    digest_hash, size_bytes = string.split('/', 1)
    return Digest(hash=digest_hash, size_bytes=int(size_bytes))
