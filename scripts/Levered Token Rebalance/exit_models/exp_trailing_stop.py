import pytz
import datetime
import time

from loguru import logger
logger.add("logs/ftx_levered_token_rebalance.log", rotation="100 MB") 

import locale
locale.setlocale( locale.LC_ALL, '' )

import numpy

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.market_order import MarketOrder

from typing import Optional, Tuple

def exp_trailing_stop(account_name: str, base: str, underlying: str, size: float, side: str, entry_price: float,
                    shape_parameter: Optional[float] = 125, min_trailing_stop_width: Optional[float] = 0.00025,
                    max_trailing_stop_width: Optional[float] = 0.01) -> Tuple[float, Optional[float]]:

    account = FtxAccount(account_name)
    exchange_feed = FtxExchangeFeed(account_name)

    _ = exchange_feed.get_ticker(underlying)
    time.sleep(3) # wait for the socket connection

    # exponential decay shape for trailing stop
    # width = max_width * exp(-shape * return)
    if side == 'sell':
        shape_parameter = -shape_parameter

    stop_f = lambda pct_change: max(min_trailing_stop_width,
                                    min(max_trailing_stop_width * numpy.exp(shape_parameter * pct_change), 
                                        max_trailing_stop_width))

    limit_level = None

    t = 0
    while True:
        market = exchange_feed.get_ticker(underlying)
        mid_point = (market['bid'] + market['ask']) / 2.

        pct_change = mid_point / entry_price - 1
        stop_width = stop_f(pct_change)

        # if we had bought, we want a trailing stop below
        if side == 'sell':
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
            if side == 'sell':
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
        return (0, None)

    else:
        order_status = order_status[0]

        while order_status.status == "open":
            time.sleep(0.1)
            order_status = session.get_order_statuses()[0]
        
        if order_status.status == "closed":
            filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
            logger.info(f'{base} | Filled {filled_size:,.4f} in {underlying} @ '
                        f'{locale.currency(order_status.average_fill_price, grouping = True)}')
            
        else:
            logger.info(f'{base} | {side.upper()} {size} {underlying} status {order_status.status}')

    
    return (order_status.filled_size, order_status.average_fill_price)