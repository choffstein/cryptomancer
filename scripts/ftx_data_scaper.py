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

from loguru import logger
logger.add("logs/ftx_data_scraper.log", rotation="100 MB") 

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

        # find the last update time
        start_time = None
        if db_market.lastPriceUpdate:
            start_time = pytz.utc.localize(db_market.lastPriceUpdate).timestamp()

        # now get all the prices for this market; we'll assume 1-minute bars
        try:
            historical_prices = ftx_client.get_historical_data(market['name'], resolution = 60, 
                                                            limit = 60*24*10, start_time = start_time, end_time = None)
        except Exception as e:
            logger.warning("Exception – " + str(e))
            historical_prices = []

        last_price_update = None
        for historical_price in historical_prices:
            historical_price = {key: historical_price[key] for key in price_table_columns}
            historical_price['startTime'] = pandas.Timestamp(historical_price['startTime']).to_pydatetime()
            historical_price['market_id'] = db_market.id

            if not last_price_update:
                last_price_update = historical_price['startTime']
            else:
                last_price_update = max(last_price_update, historical_price['startTime'])

            historical_price['lastUpdated'] = datetime.datetime.utcnow()
            db_price = db.Price(**historical_price)
            session.add(db_price)

        if last_price_update:
            db_market.lastPriceUpdate = last_price_update


def _update_funding_rates(perpetual: str):
    ftx_client = ftx.FtxClient()

    funding_rate_columns = db.FundingRate.__table__.columns.keys()
    funding_rate_columns.remove('id')
    funding_rate_columns.remove('market_id')
    funding_rate_columns.remove('lastUpdated')

    with db.session_scope() as session:
        db_market = session.query(db.Market).filter_by(name = perpetual).first()
        
        start_time = None
        if db_market.lastFundingRateUpdate:
            start_time = pytz.utc.localize(db_market.lastFundingRateUpdate).timestamp()
        
        try:
            historical_funding_rates = ftx_client.get_funding_rates(perpetual, start_time = start_time, end_time = None)

        except Exception as e:
            logger.exception(e)
            historical_funding_rates = []

        last_funding_rate_update = None
        for historical_funding_rate in historical_funding_rates:
            historical_funding_rate = {key: historical_funding_rate[key] for key in funding_rate_columns}
            historical_funding_rate['time'] = pandas.Timestamp(historical_funding_rate['time']).to_pydatetime()
            historical_funding_rate['market_id'] = db_market.id
            historical_funding_rate['lastUpdated'] = datetime.datetime.utcnow()

            if not last_funding_rate_update:
                last_funding_rate_update = historical_funding_rate['time']
            else:
                last_funding_rate_update = max(last_funding_rate_update, historical_funding_rate['time'])
            
            db_funding_rate = db.FundingRate(**historical_funding_rate)
            session.add(db_funding_rate)

        if last_funding_rate_update:
            db_market.lastFundingRateUpdate = last_funding_rate_update

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Update FTX database.')
    parser.add_argument('--sleep', dest='sleep', action='store', default=300,
                        help='How long to sleep between refresh cycles')

    args = parser.parse_args()

    ftx_client = ftx.FtxClient()

    while True:
        start = time.time()
        logger.info("Starting scraping process...")

        # get all the futures contracts and add/update their contract
        # definitions in our database
    
        try:
            markets = ftx_client.get_markets()
            logger.info("Updating markets and prices")
            parallel.lmap(_update, markets)

            # figure out which contracts are perpetuals so we can 
            # get their related funding rates
            perpetuals = [market['name'] for market in 
                                filter(lambda market: '-PERP' in market['name'], markets)]
            logger.info("Updating funding rates")
            parallel.lmap(_update_funding_rates, perpetuals)

        except Exception as e:
            logger.exception("Exception – " + str(e))
            # TODO: Handle connection issues?
            pass

        logger.info("Going to sleep...")
        end = time.time()

        # re-run every 5 minutes
        time.sleep(max(0, args.sleep - (end - start)))

    Session.remove()
    