"""Create Contracts Table

Revision ID: 97dc78afe219
Revises: 3b608edf46ae
Create Date: 2021-05-18 10:23:58.523030

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '97dc78afe219'
down_revision = '3b608edf46ae'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('contracts',
                sa.Column('id', sa.Integer, primary_key = True),
                sa.Column('exchange_id', sa.Integer, nullable = False),
                sa.Column('market_id', sa.Integer, nullable = False),
                sa.Column('name', sa.String(32), nullable = False),
                sa.Column('underlying', sa.String(32), nullable = False),
                sa.Column('type', sa.String(32), nullable = False),
                sa.Column('perpetual', sa.Boolean),
                sa.Column('postOnly', sa.Boolean),
                sa.Column('expiry', sa.DateTime()),
                sa.Column('expired', sa.Boolean),
                sa.Column('enabled', sa.Boolean),
                sa.Column('priceIncrement', sa.Float),
                sa.Column('sizeIncrement', sa.Float),
                sa.Column('positionLimitWeight', sa.Float)
    )
    op.create_foreign_key(None, 'contracts', 'exchanges', ['exchange_id'], ['id'])
    op.create_foreign_key(None, 'contracts', 'markets', ['market_id'], ['id'])


def downgrade():
    op.drop_table('contracts')