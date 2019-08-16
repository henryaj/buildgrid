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

"""Add a way to mark a job as assigned without changing stage

Revision ID: c3032fa2308f
Revises: 587e3f62158c
Create Date: 2019-08-14 13:36:12.607085

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c3032fa2308f'
down_revision = '587e3f62158c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('jobs', sa.Column('assigned', sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column('jobs', 'assigned')
