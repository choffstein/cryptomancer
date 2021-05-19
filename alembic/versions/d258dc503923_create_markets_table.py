"""Create Markets Table

Revision ID: d258dc503923
Revises: d2b271b5234e
Create Date: 2021-05-11 07:24:53.493334

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd258dc503923'
down_revision = 'd2b271b5234e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('markets',
                sa.Column('id', sa.Integer, primary_key = True),
                sa.Column('exchange_id', sa.Integer, nullable = False),
                sa.Column('name', sa.String(32), nullable = False),
                sa.Column('enabled', sa.Boolean),
                sa.Column('postOnly', sa.Boolean),
                sa.Column('priceIncrement', sa.Float),
                sa.Column('sizeIncrement', sa.Float),
                sa.Column('minProvideSize', sa.Float),
                sa.Column('type', sa.String(32), nullable = False),
                sa.Column('baseCurrency', sa.String(32)),
                sa.Column('quoteCurrency', sa.String(32)),
                sa.Column('underlying', sa.String(32)),
                sa.Column('lastPriceUpdate', sa.DateTime()),
                sa.Column('lastFundingRateUpdate', sa.DateTime())
    )
    op.create_foreign_key(None, 'markets', 'exchanges', ['exchange_id'], ['id'])

def downgrade():
    op.drop_table('markets')