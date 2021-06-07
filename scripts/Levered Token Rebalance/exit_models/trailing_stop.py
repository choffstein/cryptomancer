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
from cryptomancer.execution_handler.trailing_stop_order import TrailingStopOrder

from typing import Optional, Tuple

def trailing_stop(account_name: str, base: str, underlying: str, size: float, side: str, 
                    trail_value: float) -> Tuple[float, Optional[float]]:

    account = FtxAccount(account_name)

    # should probably auto limit order this in a loop to avoid
    # creating too much impact
    with execution_scope(wait = True) as session:   
        logger.info(f'{base} | Trailing Stop {side.upper()} {size:,.4f} w/ trail {locale.currency(trail_value, grouping = True)}')

        underlying_order = TrailingStopOrder(account = account,
                                        market = underlying,
                                        side = side,
                                        size = size,
                                        trail_value = trail_value,
                                        reduce_only = True)

        session.add(underlying_order)

    
    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # THIS IS PROBABLY A NO GOOD, VERY BAD THING AND NEEDS TO BE
        # DEALT WITH SOME HOW
        logger.info(f'{base} | {side.upper()} {size:,.4f} {underlying} FAILED')
        return (0, None)

    else:
        order_status = order_status[0]

        while order_status.status == "open":
            time.sleep(0.1)
            order_status = session.get_order_statuses()[0]
        
        if order_status.status == "closed":
            filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
            if filled_size > 1e-8:
                logger.info(f'{base} | Filled {filled_size:,.4f} in {underlying} @ '
                        f'{locale.currency(order_status.average_fill_price, grouping = True)}')

            elif order_status.order_id == -1:
                logger.info(f'{base} | Attemped {side.upper()} {size:,.4f} | Exception: {order_status.exception}')

            
        else:
            logger.info(f'{base} | {side.upper()} {size:,.4f} {underlying} status {order_status.status}')


    return (order_status.filled_size, order_status.average_fill_price)