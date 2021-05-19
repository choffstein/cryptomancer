"""Create Exchanges Table

Revision ID: d2b271b5234e
Revises: 
Create Date: 2021-05-11 07:21:41.135060

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd2b271b5234e'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('exchanges',
                sa.Column('id', sa.Integer, primary_key = True),
                sa.Column('name', sa.String(32), nullable = False)
    )

def downgrade():
    op.drop_table('exchanges')