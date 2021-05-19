import os

from contextlib import contextmanager

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

import cryptomancer.local_secrets as local_secrets

sql_secrets = local_secrets.load("postgres")

SQL_URL = sql_secrets['SQL_URL']
SQL_PORT = sql_secrets['SQL_PORT']
SQL_DB = sql_secrets['SQL_DB']
SQL_USER = sql_secrets['SQL_USER']
SQL_PASSWORD = sql_secrets['SQL_PASSWORD']

engine = create_engine(f'postgresql://{SQL_USER}:{SQL_PASSWORD}@{SQL_URL}:{SQL_PORT}/{SQL_DB}')

# auto reflect the properties of the tables
# we created with alembic
Base = automap_base()
Base.prepare(engine, reflect = True)

Exchange = Base.classes.exchanges
Market = Base.classes.markets
Contract = Base.classes.contracts
Price = Base.classes.prices
FundingRate = Base.classes.funding_rates

session_factory = sessionmaker(bind = engine)
Session = scoped_session(session_factory)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
