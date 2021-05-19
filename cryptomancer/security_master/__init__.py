import pandas
from typing import Optional
import collections
import datetime

import cryptomancer.security_master.db as db

class SecurityMaster(object):
    def __init__(self, exchange_name: str):
        self._exchange_name = exchange_name

    def get_market_spec(self, market_name: str) -> db.Market:
        with db.session_scope() as session:
            exchange_db = session.query(db.Exchange).filter_by(name = self._exchange_name).first()
            return session.query(db.Market).filter_by(exchange_id = exchange_db.id,
                                                        name = market_name).first().__dict__


    def get_contract_spec(self, contract_name: str) -> db.Contract:
        with db.session_scope() as session:
            exchange_db = session.query(db.Exchange).filter_by(name = self._exchange_name).first()
            return session.query(db.Contract).filter_by(exchange_id = exchange_db.id,
                                                        name = contract_name).first().__dict__


    def get_prices(self, market_name: str, 
                                start: Optional[datetime.datetime] = None, 
                                end: Optional[datetime.datetime] = None) -> pandas.DataFrame:

        if not start:
            start = datetime.datetime(1900, 1, 1)
        
        if not end: 
            end = datetime.datetime(2100, 1, 1)

        with db.session_scope() as session:
            market_spec = self.get_market_spec(market_name)

            prices = session.query(db.Price).filter(db.Price.market_id == market_spec['id'],
                                                    db.Price.startTime.between(start, end)).all()

            # startTime, open, high, low, close, volume
            df = collections.defaultdict(dict)
            for row in prices:
                df[row.startTime] = {'open': row.open, 'high': row.high, 'low': row.low, 'close': row.close, 'volume': row.volume}
    
            df = pandas.DataFrame.from_dict(df, orient = 'index')
            df.sort_index(inplace=True)

        return df

    def get_funding_rates(self, market_name: str,
                                start: Optional[datetime.datetime] = None, 
                                end: Optional[datetime.datetime] = None) -> pandas.DataFrame:
        
        if not start:
            start = datetime.datetime(1900, 1, 1)
        
        if not end: 
            end = datetime.datetime(2100, 1, 1)
        
        with db.session_scope() as session:
            market_spec = self.get_market_spec(market_name)

            funding_rates = session.query(db.FundingRate).filter(db.FundingRate.market_id == market_spec['id'],
                                                                db.FundingRate.time.between(start, end)).all()

            df = collections.defaultdict(dict)
            for row in funding_rates:
                df[row.time] = {'rate': row.rate, 'lastUpdated': row.lastUpdated}

            df = pandas.DataFrame.from_dict(df, orient = 'index')
            df.sort_index(inplace=True)

        return df