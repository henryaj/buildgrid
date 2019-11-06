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

"""Add index

Revision ID: 8a0dbc05b5b0
Revises: 26570f8fa002
Create Date: 2019-10-01 10:23:27.720996

"""
from datetime import datetime

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8a0dbc05b5b0'
down_revision = '26570f8fa002'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'index',
        sa.Column('digest_hash', sa.String(), nullable=False),
        sa.Column('digest_size_bytes', sa.Integer(), nullable=False),
        sa.Column('accessed_timestamp', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('digest_hash')
    )
    op.create_index(op.f('ix_index_digest_hash'),
                    'index', ['digest_hash'], unique=True)
    op.create_index(op.f('ix_index_accessed_timestamp'),
                    'index', ['accessed_timestamp'], unique=False)


def downgrade():
    op.drop_index(op.f('ix_index_accessed_timestamp'), table_name='index')
    op.drop_index(op.f('ix_index_digest_hash'), table_name='index')
    op.drop_table('index')
