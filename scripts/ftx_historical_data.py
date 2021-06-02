import argparse

import ftx
import pandas

import time
import datetime
import pytz

import tqdm
from functools import reduce

import numbers
import numpy

import cryptomancer.security_master.db as db
import cryptomancer.parallel as parallel

from functools import wraps

import gc

from loguru import logger
logger.add("logs/ftx_data_scraper.log", rotation="100 MB") 

from sqlalchemy import asc

def _isequal(a, b):
    """
    Check whether a and b are equal to each other, taking special care 
    if a and b are Numbers and we need to account for numerical
    precision issues.
    """
    if isinstance(a, numbers.Number) and isinstance(b, numbers.Number):
        return numpy.isclose(a, b)
    else:
        return a == b


def _new_or_update(session, klass, record):
    db_record = session.query(klass).filter_by(name = record['name']).first()

    # do we already have the contract in our database?
    if db_record:
        # is anything actually different?
        b = reduce(lambda x, y: x and y, 
                        [_isequal(record[key], getattr(db_record, key)) for key in record.keys()])
        if not b:
            # update the data; 
            # normally we'd just do db_record.update(record)
            # but the reflection code isn't allowing it...
            session.query(klass).filter_by(id = db_record.id).update(record)
    else:
        # create the record
        db_record = klass(**record)
        session.add(db_record)

        # we need to commit to to have the id
        session.commit()

    return db_record


def retries(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        retries = 3
        while True:
            try:
                return f(*args, **kwargs)
            except:
                if retries == 0:
                    raise
                else:
                    retries = retries - 1
                    time.sleep(1)
    return wrapper

@retries
def _update(market: dict):
    ftx_client = ftx.FtxClient()

    market_table_columns = db.Market.__table__.columns.keys()
    market_table_columns.remove('id')
    market_table_columns.remove('exchange_id')
    market_table_columns.remove('lastPriceUpdate')
    market_table_columns.remove('lastFundingRateUpdate')

    contract_table_columns = db.Contract.__table__.columns.keys()
    contract_table_columns.remove('id')
    contract_table_columns.remove('exchange_id')
    contract_table_columns.remove('market_id')

    price_table_columns = db.Price.__table__.columns.keys()
    price_table_columns.remove('id')
    price_table_columns.remove('market_id')
    price_table_columns.remove('lastUpdated')

    with db.session_scope() as session:
        exchange_db = session.query(db.Exchange).filter_by(name = 'FTX').first()

        # only keep the columns we actually need for our SQL table
        market = {key: market[key] for key in market_table_columns}
        market['exchange_id'] = exchange_db.id

        db_market = _new_or_update(session, db.Market, market)

        if market['type'] in {'perpetual', 'future'}:
            future = ftx_client.get_future(market['name'])
            future = {key: future[key] for key in contract_table_columns}
            future['exchange_id'] = exchange_db.id
            future['market_id'] = db_market.id

            db_future = _new_or_update(session, db.Contract, future) 

        #start_time = pytz.utc.localize(datetime.datetime(2015, 12, 31)).timestamp() #pytz.utc.localize(datetime.datetime(2016, 12, 31)).timestamp()
        logger.info(f"Getting oldest price for {market['name']}")
        oldest_price_record = session.query(db.Price).filter_by(market_id = db_market.id).order_by(asc(db.Price.startTime)).first()
             
        if oldest_price_record is None:
            end_time = datetime.datetime.utcnow().timestamp()
        else:
            end_time = pytz.utc.localize(oldest_price_record.startTime).timestamp()
        
        limit = 5000
        resolution = 60

        historical_prices = []

        while True:    
            logger.info(f"{market['name']} end time: " + str(datetime.datetime.fromtimestamp(end_time)))
            price_slice = ftx_client.get_historical_data(market['name'], resolution = resolution, 
                                                            limit = limit, start_time = None, end_time = end_time)

            if len(price_slice) == 1:
                logger.info(f"No prices for {market['name']} older than " + str(datetime.datetime.fromtimestamp(end_time)))
                break
            
            for historical_price in price_slice:
                historical_price = {key: historical_price[key] for key in price_table_columns}
                historical_price['startTime'] = pandas.Timestamp(historical_price['startTime']).to_pydatetime()
                historical_price['market_id'] = db_market.id

                # make sure end_time keeps getting pulled back
                timestamp = historical_price['startTime'].timestamp()
                if timestamp < end_time:
                    end_time = timestamp

                historical_price['lastUpdated'] = datetime.datetime.utcnow()
                db_price = db.Price(**historical_price)
                session.add(db_price)
        
            session.commit()

    gc.collect()

@retries
def _update_funding_rates(perpetual: str):
    ftx_client = ftx.FtxClient()

    funding_rate_columns = db.FundingRate.__table__.columns.keys()
    funding_rate_columns.remove('id')
    funding_rate_columns.remove('market_id')
    funding_rate_columns.remove('lastUpdated')

    with db.session_scope() as session:
        db_market = session.query(db.Market).filter_by(name = perpetual).first()
        
        start_time = pytz.utc.localize(datetime.datetime(2015, 12, 31)).timestamp() #pytz.utc.localize(datetime.datetime(2016, 12, 31)).timestamp()
        
        oldest_rate_record = session.query(db.FundingRate).filter_by(market_id = db_market.id).order_by(asc(db.FundingRate.time)).first()
        if oldest_rate_record is None:
            end_time = datetime.datetime.utcnow().timestamp()
        else:
            end_time = pytz.utc.localize(oldest_rate_record.time).timestamp()

        while True:
            logger.info(f"{perpetual} funding rate end time: " + str(datetime.datetime.fromtimestamp(end_time)))
            funding_rate_slice = ftx_client.get_funding_rates(perpetual, start_time = start_time, end_time = end_time)

            # we do 1 here because end-time is inclusive
            if len(funding_rate_slice) == 1:
                logger.info(f"No funding rates {perpetual} prior to " + str(datetime.datetime.fromtimestamp(end_time)))
                break

            for historical_funding_rate in funding_rate_slice:
                historical_funding_rate = {key: historical_funding_rate[key] for key in funding_rate_columns}
                historical_funding_rate['time'] = pandas.Timestamp(historical_funding_rate['time']).to_pydatetime()
                historical_funding_rate['market_id'] = db_market.id
                historical_funding_rate['lastUpdated'] = datetime.datetime.utcnow()

                timestamp = historical_funding_rate['time'].timestamp()
                if timestamp < end_time:
                    end_time = timestamp

                db_funding_rate = db.FundingRate(**historical_funding_rate)
                session.add(db_funding_rate)

            session.commit()

if __name__ == '__main__':

    ftx_client = ftx.FtxClient()

    # get all the futures contracts and add/update their contract
    # definitions in our database
    try:
        markets = ftx_client.get_markets()
        logger.info("Updating markets and prices")
        #ada_perp = list(filter(lambda x: x['name'] == 'ADA-PERP', markets))[0]
        #_update(ada_perp)
        #parallel.lmap(_update, markets, progress_bar = True)

        # figure out which contracts are perpetuals so we can 
        # get their related funding rates
        perpetuals = [market['name'] for market in 
                            filter(lambda market: '-PERP' in market['name'], markets)]
        logger.info("Updating funding rates")
        parallel.lmap(_update_funding_rates, perpetuals)

    except Exception as e:
        logger.exception(str(e))
        raise


    