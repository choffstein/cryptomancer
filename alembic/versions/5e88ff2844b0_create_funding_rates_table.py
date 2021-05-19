"""Create Funding Rates Table

Revision ID: 5e88ff2844b0
Revises: 97dc78afe219
Create Date: 2021-05-18 22:04:00.022875

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5e88ff2844b0'
down_revision = '97dc78afe219'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('funding_rates',
                sa.Column('id', sa.Integer, primary_key = True),
                sa.Column('market_id', sa.Integer, nullable = False),
                sa.Column('future', sa.String(32), nullable = False),
                sa.Column('rate', sa.Float, nullable = False),
                sa.Column('time', sa.DateTime(), nullable = False),
                sa.Column('lastUpdated', sa.DateTime(), nullable = False),

    )
    op.create_foreign_key(None, 'funding_rates', 'markets', ['market_id'], ['id'])


def downgrade():
    op.drop_table('funding_rates')
