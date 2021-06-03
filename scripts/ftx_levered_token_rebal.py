import sys
from optparse import OptionParser

import ftx
import pandas
import numpy

import datetime
import pytz

import cryptomancer.parallel

import time

from loguru import logger
logger.add("logs/ftx_static_cash_and_carry_perpetual.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster
from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.limit_order import LimitOrder
from cryptomancer.execution_handler.auto_limit_order import AutoLimitOrder
from cryptomancer.execution_handler.trailing_stop_order import TrailingStopOrder


def run(args):
    base, proxy, account_name, dollar_target, trail_stop_width = args

    try:
        account = FtxAccount(account_name)
        exchange_feed = FtxExchangeFeed(account_name)
    except Exception as e:
        logger.exception(e)
        sys.exit(0)

    ftx_client = ftx.FtxClient()
    levered_tokens = ftx_client.get_levered_tokens()

    underlying = f'{base}-PERP'

    # subscribe to bid/ask
    _ = exchange_feed.get_ticker(underlying)
    _ = exchange_feed.get_ticker(f'{proxy}-PERP')

    if proxy == 'BTC':
        tokens_to_keep = ['BULL', 'BEAR', 'HALF', 'HEDGE']
    else:
        tokens_to_keep = [proxy + token for token in ['BULL', 'BEAR', 'HALF', 'HEDGE']]

    kept_tokens = [token for token in levered_tokens if token['name'] in tokens_to_keep]

    ##### SLEEP UNTIL READY TO RUN
    now = pytz.utc.localize(datetime.datetime.utcnow())
    if now.hour == 0:
        tomorrow = now
    else:
        tomorrow = now + datetime.timedelta(days = 1)

    ##### REBAL TIME IS AT 00:02:00 UTC, BUT IN PRACTICE
    ##### IT TENDS TO ACTUALLY FIRE OFF AFTER 00:02:20
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 20, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance - 45:.2f}s...")
    time.sleep(time_until_rebalance - 45)
    logger.info(f"{base} | Awake and ready to trade!")
    
    ##### GO THROUGH THE LEVERED TOKEN AND FIGURE OUT 
    ##### HOW MUCH NAV NEEDS TO BE REBALANCED
    market = ftx_client.get_market(f'{proxy}-PERP')
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

    logger.info(f'{base} | Expected Rebalance of {proxy}-PERP: ${nav_to_rebal:.2f} / {underlying_to_rebal:2f} shares')

    ##### EXECUTE A LIMIT ORDER
    side = 'buy' if underlying_to_rebal > 1e-8 else 'sell'
    for retry in range(5):

        # get current mid point to figure out size
        market = exchange_feed.get_ticker(underlying)
        mid_point = (market['bid'] + market['ask']) / 2.
        width = (market['ask'] - market['bid']) / mid_point
        
        size = dollar_target / mid_point
        
        if retry < 4:
            logger.info(f'{base} | Attempt #{retry + 1} at providing liquidity...')

            try:
                with execution_scope(wait = True, timeout = 10) as session:
                    underlying_order = LimitOrder(account = account,
                                                    market = underlying,
                                                    side = side,
                                                    size = size,
                                                    price = mid_point * (1 - width / 2),
                                                    post_only = True)
                    session.add(underlying_order)
            
            except TimeoutError:
                continue

        else:
            logger.info(f'{base} | Failed {retry} times; now trying to take liquidity...')
            with execution_scope(wait = True) as session:
                underlying_order = AutoLimitOrder(account = account,
                                                exchange_feed = exchange_feed,
                                                market = underlying,
                                                side = side,
                                                size = size,
                                                width = width)
                session.add(underlying_order)
            
        ##### MAKE SURE THE TRADE ACTUALLY WENT THROUGH
        order_status = session.get_order_statuses()
        if len(order_status) == 0:
            # TRADE MUST'VE BEEN CANCELLED, RETURN
            logger.info(f'{base} | {side.upper()} {size:.4f} {underlying} FAILED')
            return

        else:
            # TRADE WAS GOOD; GET THE TOTAL FILL SIZE
            order_status = order_status[0]

            filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
            if abs(filled_size) < 1e-8:
                continue

            logger.info(f'{base} | Attemped {side.upper()} {size:.4f} | FILLED {filled_size}')
            break
    else:
        logger.info(f'{base} | Failed to execute initial order.  Bailing...')
        return
    
    # WE HAVE TO GO BACK TO SLEEP HERE JUST IN CASE WE GO FILLED TOO EARLY; WE
    # DON'T WANT OUR STOP LOSS TRIGGERING TOO EARLY
    now = pytz.utc.localize(datetime.datetime.utcnow())
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 30, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance:.2f}s...")
    time.sleep(time_until_rebalance)
    logger.info(f"{base} | Awake and ready to put on the stop!")

    ##### WITH THE FILLED SIZE, SET A TRAILING STOP SELL 
    ##### TO TRADE ANY MOMENTUM
    size = abs(filled_size)
    with execution_scope(wait = True) as session:
        side = 'sell' if underlying_to_rebal > 1e-8 else 'buy'
    
        logger.info(f'{base} | Attempting to {side.upper()} {size}')
        underlying_order = TrailingStopOrder(account = account,
                                        exchange_feed = exchange_feed,
                                        market = underlying,
                                        side = side,
                                        size = size,
                                        width = trail_stop_width,
                                        reduce_only = True) 
        session.add(underlying_order)

    ##### CHECK THE ORDER STATUS AGAIN
    while True:
        order_status = session.get_order_statuses()
        if len(order_status) == 0:
            # THIS IS PROBABLY A NO GOOD, VERY BAD THING AND NEEDS TO BE
            # DEALT WITH SOME HOW
            logger.info(f'{base} | {side.upper()} {size} {underlying} FAILED')
            break
        else:
            order_status = order_status[0]
            if order_status.status == "triggered":
                filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
                logger.info(f'{base} | Filled {filled_size:.4f} in {underlying}')
                break
            else:
                time.sleep(1)


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Dollar Target>"
    parser = OptionParser(usage = usage)
    (options, args) = parser.parse_args()

    account_name = args[0]
    dollar_target = float(args[1])

    sm = SecurityMaster("FTX")

    """
    min_size = {}
    min_price_increment = {}
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'SOL']:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']
    """

    trail_stop = {
        'BTC': 0.00125,
        'ETH': 0.00125,
        'DOGE': 0.0035,
        'MATIC': 0.00125,
        'ADA': 0.00125,
        'SOL': 0.00125
    }

    proxy = {
        'BTC': 'BTC',
        'ETH': 'BTC',
        'DOGE': 'BTC',
        'MATIC': 'BTC',
        'ADA': 'BTC',
        'SOL': 'BTC'
    }

    parameters = []
    for underlying in ['BTC', 'DOGE', 'MATIC', 'ADA', 'SOL']:
        parameters.append((underlying, proxy[underlying], account_name, dollar_target, trail_stop[underlying] * 3))
    
    #run(parameters[-1])
    cryptomancer.parallel.lmap(run, parameters)
