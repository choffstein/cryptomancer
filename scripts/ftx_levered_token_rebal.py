import sys
from optparse import OptionParser

import ftx
import pandas
import numpy

import datetime
import pytz

import quantlab.parallel

import time

from loguru import logger
logger.add("logs/ftx_static_cash_and_carry_perpetual.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster
from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.limit_order import LimitOrder


def run(args):
    base, account_name, dollar_target, min_size, min_price_increment = args

    try:
        account = FtxAccount(account_name)
        exchange_feed = FtxExchangeFeed(account_name)
    except Exception as e:
        logger.exception(e)
        sys.exit(0)


    ftx_client = ftx.FtxClient()
    levered_tokens = ftx_client.get_levered_tokens()

    underlying = f'{base}-PERP'

    now = pytz.utc.localize(datetime.datetime.utcnow())
    if now.hour == 0:
        tomorrow = now
    else:
        tomorrow = now + datetime.timedelta(days = 1)

    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 15, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance - 15}s...")

    # wake up 30s before time until rebalance
    time.sleep(time_until_rebalance - 30)

    logger.info(f"{base} | Awake and ready to trade!")

    # subscribe to trades
    #_ = exchange_feed.get_trades(underlying)

    if base == 'BTC':
        tokens_to_keep = ['BULL', 'BEAR', 'HALF', 'HEDGE']
    else:
        tokens_to_keep = [base + token for token in ['BULL', 'BEAR', 'HALF', 'HEDGE']]

    kept_tokens = [token for token in levered_tokens if token['name'] in tokens_to_keep]

    market = ftx_client.get_market(underlying)
    mid_point = (market['bid'] + market['ask']) / 2.

    nav_to_rebal = 0
    for token in kept_tokens:
        leverage = token['leverage']
        nav = token['totalNav']

        major_rebalances = ftx_client.get_token_rebalances(token['name'])
        last_rebalance = major_rebalances[0]

        fill_price = last_rebalance['avgFillPrice']
        r = (mid_point / fill_price) - 1

        nav_to_rebal = nav_to_rebal + nav * (leverage**2 - leverage) * r

    underlying_to_rebal = nav_to_rebal / mid_point

    market = ftx_client.get_market(underlying)
    mid_point = (market['bid'] + market['ask']) / 2.
    
    size = dollar_target / mid_point

    # they do $4,000,000 orders
    n_orders = abs(nav_to_rebal) / 4000000
    n_orders = int(n_orders) + 1

    with execution_scope() as session:
        side = 'buy' if underlying_to_rebal > 1e-8 else 'sell'
    
        logger.info(f'{base} | {side.upper()} {size}')
        underlying_order = LimitOrder(account = account,
                                        exchange_feed = exchange_feed,
                                        market = underlying,
                                        side = side,
                                        size = size,
                                        width = 0.005)
        session.add(underlying_order)
        

    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # the order errored out
        logger.info(f'{base} | {side.upper()} {size} {underlying} FAILED')
        return

    else:
        order_status = order_status[0]

        # figure out how much of the order was actually filled
        filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
        logger.info(f'{base} | Filled {filled_size} in {underlying}')
    
    # start at 00:02:20
    # 2nd trade is at 00:02:40
    # 3rd+ trade is every 10s after
    max_time = 60
    if n_orders > 3:
        max_time = max_time + 10 * (n_orders - 2)

    logger.info(f'{base} | Sleeping for {max_time}s...')

    time.sleep(max_time)

    logger.info(f'{base} | Done sleeping; liquidating.')

    size = abs(filled_size)
    with execution_scope() as session:
        side = 'sell' if underlying_to_rebal > 1e-8 else 'buy'
    
        logger.info(f'{base} | {side.upper()} {size}')
        underlying_order = LimitOrder(account = account,
                                        exchange_feed = exchange_feed,
                                        market = underlying,
                                        side = side,
                                        size = size)
        session.add(underlying_order)

    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # the order errored out
        logger.info(f'{base} | {side.upper()} {size} {underlying} FAILED')
    else:
        order_status = order_status[0]

        # figure out how much of the order was actually filled
        filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
        logger.info(f'{base} | Filled {filled_size} in {underlying}')


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Dollar Target>"
    parser = OptionParser(usage = usage)
    (options, args) = parser.parse_args()

    account_name = args[0]
    dollar_target = float(args[1])

    sm = SecurityMaster("FTX")

    min_size = {}
    min_price_increment = {}
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'SOL']:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']

    parameters = []
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'SOL']:
        parameters.append((underlying, account_name, dollar_target, min_size[underlying], min_price_increment[underlying]))
    
    quantlab.parallel.lmap(run, parameters)