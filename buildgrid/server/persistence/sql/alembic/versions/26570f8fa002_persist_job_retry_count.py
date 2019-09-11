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

"""Persist job retry count

Revision ID: 26570f8fa002
Revises: bbe7e36a3717
Create Date: 2019-09-11 13:24:06.007005

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '26570f8fa002'
down_revision = 'bbe7e36a3717'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('jobs', sa.Column('n_tries', sa.Integer(), default=0))


def downgrade():
    op.drop_column('jobs', 'n_tries')
