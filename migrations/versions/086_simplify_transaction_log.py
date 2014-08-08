"""simplify transaction log

Revision ID: 8c2406df6f8
Revises:294200d809c8
Create Date: 2014-08-08 01:57:17.144405

"""

# revision identifiers, used by Alembic.
revision = '8c2406df6f8'
down_revision = '294200d809c8'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql


def upgrade():
    op.alter_column('transaction', 'public_snapshot',
                    new_column_name='snapshot',
                    existing_type=sa.Text(length=4194304))
    op.drop_column('transaction', 'private_snapshot')
    op.drop_column('transaction', 'delta')
    op.drop_column('transaction', 'object_public_id')


def downgrade():
    raise Exception("You shouldn't want to roll back from this one, but if "
                    "you do, comment this out")
    op.alter_column('transaction', 'snapshot',
                    new_column_name='public_snapshot')
    op.add_column('transaction', sa.Column('delta', mysql.LONGTEXT(),
                                           nullable=True))
    op.add_column('transaction', sa.Column('private_snapshot',
                                           mysql.LONGTEXT(), nullable=True))
    op.add_column('transaction', sa.Column('object_public_id', sa.String,
                                           nullable=True))
