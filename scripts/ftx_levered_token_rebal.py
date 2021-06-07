import sys
from optparse import OptionParser

import ftx
import pandas
import numpy

import datetime
import pytz

import cryptomancer.parallel

import time

import locale
locale.setlocale( locale.LC_ALL, '' )

from loguru import logger
logger.add("logs/ftx_static_cash_and_carry_perpetual.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster
from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.market_order import MarketOrder
from cryptomancer.execution_handler.limit_order import LimitOrder
from cryptomancer.execution_handler.auto_limit_order import AutoLimitOrder


def run(args):
    base, proxy, account_name, dollar_target, max_trailing_stop_width, min_trade_size = args

    try:
        account = FtxAccount(account_name)
        exchange_feed = FtxExchangeFeed(account_name)
    except Exception as e:
        logger.exception(e)
        sys.exit(0)

    ftx_client = ftx.FtxClient()
    levered_tokens = ftx_client.get_levered_tokens()

    underlying = f'{base}-PERP'

    # SUBSCRIBE TO BID/ASK FEEDS
    _ = exchange_feed.get_ticker(underlying)
    _ = exchange_feed.get_ticker(f'{proxy}-PERP')

    if proxy == 'BTC':
        tokens_to_keep = ['BULL', 'BEAR', 'HALF', 'HEDGE']
    else:
        tokens_to_keep = [proxy + token for token in ['BULL', 'BEAR', 'HALF', 'HEDGE']]

    kept_tokens = [token for token in levered_tokens if token['name'] in tokens_to_keep]

    # SLEEP UNTIL READY TO RUN
    now = pytz.utc.localize(datetime.datetime.utcnow())
    if now.hour == 0:
        tomorrow = now
    else:
        tomorrow = now + datetime.timedelta(days = 1)

    # REBAL TIME IS AT 00:02:00 UTC, BUT IN PRACTICE
    # IT TENDS TO ACTUALLY FIRE OFF AFTER 00:02:20
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 20, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance - 60:,.2f}s...")
    if time_until_rebalance - 60 > 0:
        time.sleep(time_until_rebalance - 60)
    logger.info(f"{base} | Awake and ready to trade!")

    _ = exchange_feed.get_ticker(underlying)
    _ = exchange_feed.get_ticker(f'{proxy}-PERP')
    
    # GO THROUGH THE LEVERED TOKEN AND FIGURE OUT 
    # HOW MUCH NAV NEEDS TO BE REBALANCED
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

    logger.info(f'{base} | Expected Rebalance of {proxy}-PERP: '
                f'{locale.currency(nav_to_rebal, grouping = True)} / {underlying_to_rebal:,.2f} shares')

    # TRY TO POST LIQUIDITY THROUGH LIMIT ORDERS
    # RETRY UP TO 5X
    # ON THE 6th TRY, JUST TAKE LIQUIDITY
    side = 'buy' if underlying_to_rebal > 1e-8 else 'sell'
    
    fills = []
    fill_prices = []

    for retry in range(6):

        # GET CURRENT MID-POINT TO FIGURE OUT SIZE
        market = exchange_feed.get_ticker(underlying)
        mid_point = (market['bid'] + market['ask']) / 2.
        width = (market['ask'] - market['bid']) / mid_point
        
        size = dollar_target / mid_point
        
        if retry < 5:
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
            
        # MAKE SURE THE TRADE ACTUALLY WENT THROUGH
        order_status = session.get_order_statuses()
        if len(order_status) == 0:
            # TRADE MUST'VE BEEN CANCELLED, RETURN
            logger.info(f'{base} | {side.upper()} {size:,.4f} {underlying} FAILED')
            return

        else:
            # TRADE WAS GOOD; GET THE TOTAL FILL SIZE
            order_status = order_status[0]

            filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
            if abs(filled_size) < 1e-8:
                continue

            fill_price = order_status.average_fill_price
            # SHOULD WE CHECK IF THE FILLED SIZE < OUR TARGET FILLED SIZE AND TRY TO FILL MORE?
            # OR SINCE WE'RE ON A TIME CONSTRAINT, JUST LET IT GO?
            logger.info(f'{base} | Attemped {side.upper()} {size:,.4f} | FILLED {filled_size:,.4f}'
                        f' @ {locale.currency(fill_price, grouping = True)}')

            fills.append(filled_size)
            fill_prices.append(fill_price)

            # did we fill everything?  if not, reduce our dollar target and try again
            if retry == 5 or ((dollar_target - filled_size * fill_price) / mid_point < min_trade_size):
                break
            else:
                dollar_target = dollar_target - filled_size * fill_price
                logger.info(f'{base} | Partial fill, trying to fill the rest...')
    else:
        logger.info(f'{base} | Failed to execute entry order.  Bailing...')
        return
    
    total_fill = numpy.sum(fills)
    average_fill_price = numpy.dot(fills, fill_prices) / numpy.sum(fills)

    # WE HAVE TO GO BACK TO SLEEP HERE JUST IN CASE WE GO FILLED TOO EARLY; WE
    # DON'T WANT OUR STOP LOSS TRIGGERING TOO EARLY
    now = pytz.utc.localize(datetime.datetime.utcnow())
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 30, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance:,.2f}s...")
    if time_until_rebalance > 0:
        time.sleep(time_until_rebalance)
    logger.info(f"{base} | Awake and ready to put on the stop!")

    # WITH THE FILLED SIZE, SET A TRAILING STOP SO WE CAN
    # TRY TO BENEFIT FROM ANY MOMENTUM THAT OCCURS
    # THIS CODE IS LIQUIDITY TAKING, BUT LIKELY CHEAPER
    # THAN TRYING TO CHASE MARKETS WITH POST ORDERS
    size = abs(total_fill)

    # exponential decay shape for trailing stop
    # width = max_width * exp(-shape * return)
    if underlying_to_rebal > 1e-8:
        shape_parameter = -125
    else:
        shape_parameter = 125

    stop_f = lambda pct_change: max(0.00025, #2.5bp minimum stop 
                                    min(max_trailing_stop_width * numpy.exp(shape_parameter * pct_change), 
                                        max_trailing_stop_width))

    limit_level = None
    side = 'sell' if underlying_to_rebal > 1e-8 else 'buy'

    t = 0
    while True:
        market = exchange_feed.get_ticker(underlying)
        mid_point = (market['bid'] + market['ask']) / 2.

        pct_change = mid_point / average_fill_price - 1
        stop_width = stop_f(pct_change)

        # if we had bought, we want a trailing stop below
        if underlying_to_rebal > 0:
            if limit_level:
                if mid_point < limit_level: # we broke our limit
                    logger.info(f'{base} | Broke limit: {locale.currency(mid_point, grouping = True)} < '
                                    f'{locale.currency(limit_level, grouping = True)} ({stop_width:.4%})')
                    break

                limit_level = max(limit_level, mid_point * (1 - stop_width))
            else:
                limit_level = mid_point * (1 - stop_width)

        else:
            if limit_level:
                if mid_point > limit_level: # we broke our limit
                    logger.info(f'{base} | Broke limit: {locale.currency(mid_point, grouping = True)} > '
                                    f'{locale.currency(limit_level, grouping = True)} ({stop_width:.4%})')
                    break

                limit_level = min(limit_level, mid_point * (1 + stop_width))
            else:
                limit_level = mid_point * (1 + stop_width)

        t = t + 1
        # every 30 seconds, report to the logs
        if numpy.mod(t, 300) == 0:
            if underlying_to_rebal > 0:
                logger.info(f'{base} | {locale.currency(mid_point, grouping = True)} > '
                                f'{locale.currency(limit_level, grouping = True)} '
                                f'({(mid_point / limit_level - 1):.4%})')
            else:
                logger.info(f'{base} | {locale.currency(mid_point, grouping = True)} < '
                                f'{locale.currency(limit_level, grouping = True)} '
                                f'({(mid_point / limit_level - 1):.4%})')

        time.sleep(0.1)
        
    
    # should probably auto limit order this in a loop to avoid
    # creating too much impact
    with execution_scope(wait = True) as session:   
        logger.info(f'{base} | {side.upper()} {size:,.4f}')
        underlying_order = MarketOrder(account = account,
                                        market = underlying,
                                        side = side,
                                        size = size,
                                        reduce_only = True) 
        session.add(underlying_order)

    
    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # THIS IS PROBABLY A NO GOOD, VERY BAD THING AND NEEDS TO BE
        # DEALT WITH SOME HOW
        logger.info(f'{base} | {side.upper()} {size} {underlying} FAILED')

    else:
        order_status = order_status[0]
        
        if order_status.status == "closed":
            filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
            logger.info(f'{base} | Filled {filled_size:,.4f} in {underlying} @ '
                        f'{locale.currency(order_status.average_fill_price, grouping = True)}')
            
        else:
            logger.info(f'{base} | {side.upper()} {size} {underlying} status {order_status.status}')
    

if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Dollar Target>"
    parser = OptionParser(usage = usage)
    (options, args) = parser.parse_args()

    if len(args) < 2:
        print(usage)
        exit()

    account_name = args[0]
    dollar_target = float(args[1])

    sm = SecurityMaster("FTX")
    min_size = {}
    min_price_increment = {}
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'ADA', 'SOL','XRP']:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']

    trail_stop = {
        'BTC': 0.0010,
        'ETH': 0.0010,
        'DOGE': 0.0020,
        'MATIC': 0.0010,
        'ADA': 0.0010,
        'SOL': 0.0010,
        'XRP': 0.0010
    }

    trail_stop_multiplier = 3

    proxy = {
        'BTC': 'BTC',
        'ETH': 'ETH',
        'DOGE': 'ETH',
        'MATIC': 'ETH',
        'ADA': 'ETH',
        'SOL': 'ETH',
        'XRP': 'ETH'
    }

    parameters = []
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'ADA', 'SOL', 'XRP']:
        parameters.append((underlying, proxy[underlying], account_name, dollar_target, 
                                            trail_stop[underlying] * trail_stop_multiplier,
                                            min_size[underlying]))
    
    cryptomancer.parallel.lmap(run, parameters)
