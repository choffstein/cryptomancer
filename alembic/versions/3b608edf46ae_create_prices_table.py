"""Create Prices Table

Revision ID: 3b608edf46ae
Revises: d258dc503923
Create Date: 2021-05-18 08:51:26.187275

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3b608edf46ae'
down_revision = 'd258dc503923'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('prices',
                    sa.Column('id', sa.Integer, primary_key = True),
                    sa.Column('market_id', sa.Integer, nullable = False),
                    sa.Column('lastUpdated', sa.DateTime),
                    sa.Column('startTime', sa.DateTime),
                    sa.Column('open', sa.Float),
                    sa.Column('high', sa.Float),
                    sa.Column('low', sa.Float),
                    sa.Column('close', sa.Float),
                    sa.Column('volume', sa.Float)
    )
    op.create_foreign_key(None, 'prices', 'markets', ['market_id'], ['id'])


def downgrade():
    op.drop_table('prices')

