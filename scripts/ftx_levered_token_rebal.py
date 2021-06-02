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

    # subscribe to bid/ask
    _ = exchange_feed.get_ticker(underlying)

    if base == 'BTC':
        tokens_to_keep = ['BULL', 'BEAR', 'HALF', 'HEDGE']
    else:
        tokens_to_keep = [base + token for token in ['BULL', 'BEAR', 'HALF', 'HEDGE']]

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

    logger.info(f"{base} | Sleeping for {time_until_rebalance - 60:.2f}s...")
    #time.sleep(time_until_rebalance - 60)
    logger.info(f"{base} | Awake and ready to trade!")
    
    ##### GO THROUGH THE LEVERED TOKEN AND FIGURE OUT 
    ##### HOW MUCH NAV NEEDS TO BE REBALANCED
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

    logger.info(f'{base} | Expected Rebalance: ${nav_to_rebal:.2f} / {underlying_to_rebal:2f} shares')

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
            logger.info(f'{base} | Attemped {side.upper()} {size:.4f} | FILLED {filled_size}')
            break
    else:
        logger.info(f'{base} | Failed to execute initial order.  Bailing...')
        return
    
    """
    n_orders = abs(nav_to_rebal) / 4000000
    n_orders = int(n_orders) + 1

    # start at 00:02:20
    # 2nd trade is at 00:02:40
    # 3rd+ trade is every 10s after
    max_time = 60
    if n_orders > 3:
        max_time = max_time + 10 * (n_orders - 2)

    logger.info(f'{base} | Sleeping for {max_time}s...')

    time.sleep(max_time)

    logger.info(f'{base} | Done sleeping; liquidating.')
    """

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
                                        width = 0.001,       # give it 10bps of width
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
            import ipdb; ipdb.set_trace()
            order_status = order_status[0]
            if order_status.status == "closed":
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

    min_size = {}
    min_price_increment = {}
    for underlying in ['BTC']: #, 'ETH', 'DOGE', 'MATIC', 'SOL']:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']

    parameters = []
    for underlying in ['BTC']: #, 'ETH', 'DOGE', 'MATIC', 'SOL']:
        parameters.append((underlying, account_name, dollar_target, min_size[underlying], min_price_increment[underlying]))
    
    run(parameters[0])
    #cryptomancer.parallel.lmap(run, parameters)
