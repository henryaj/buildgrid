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


"""Initial schema

Revision ID: 40ebfa4555e6
Revises: None
Create Date: 2019-06-05 13:03:47.792522

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '40ebfa4555e6'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'jobs',
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('action_digest', sa.String(), nullable=False),
        sa.Column('priority', sa.Integer(), nullable=False),
        sa.Column('stage', sa.Integer(), nullable=True),
        sa.Column('do_not_cache', sa.Boolean(), nullable=False),
        sa.Column('cancelled', sa.Boolean(), nullable=False),
        sa.Column('queued_timestamp', sa.DateTime(), nullable=True),
        sa.Column('queued_time_duration', sa.Integer(), nullable=True),
        sa.Column('worker_start_timestamp', sa.DateTime(), nullable=True),
        sa.Column('worker_completed_timestamp', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('name')
    )
    op.create_index(op.f('ix_jobs_action_digest'), 'jobs', ['action_digest'], unique=False)
    op.create_index(op.f('ix_jobs_priority'), 'jobs', ['priority'], unique=False)
    op.create_index(op.f('ix_jobs_stage'), 'jobs', ['stage'], unique=False)
    op.create_table(
        'leases',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_name', sa.String(), nullable=False),
        sa.Column('status', sa.Integer(), nullable=True),
        sa.Column('state', sa.Integer(), nullable=False),
        sa.Column('worker_name', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['job_name'], ['jobs.name'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_leases_job_name'), 'leases', ['job_name'], unique=False)
    op.create_index(op.f('ix_leases_worker_name'), 'leases', ['worker_name'], unique=False)
    op.create_table(
        'operations',
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('job_name', sa.String(), nullable=False),
        sa.Column('done', sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(['job_name'], ['jobs.name'], ),
        sa.PrimaryKeyConstraint('name')
    )
    op.create_index(op.f('ix_operations_job_name'), 'operations', ['job_name'], unique=False)
    op.create_table(
        'platform_requirements',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('job_name', sa.String(), nullable=False),
        sa.Column('key', sa.String(), nullable=False),
        sa.Column('value', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['job_name'], ['jobs.name'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(
        op.f('ix_platform_requirements_key_value'),
        'platform_requirements',
        ['key', 'value'],
        unique=False
    )


def downgrade():  # pragma: no cover
    op.drop_table('platform_requirements')
    op.drop_index(op.f('ix_operations_job_name'), table_name='operations')
    op.drop_table('operations')
    op.drop_index(op.f('ix_leases_worker_name'), table_name='leases')
    op.drop_table('leases')
    op.drop_index(op.f('ix_jobs_action_digest'), table_name='jobs')
    op.drop_table('jobs')
