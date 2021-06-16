import pytz
import datetime
import time

from loguru import logger
logger.add("logs/ftx_levered_token_rebalance.log", rotation="100 MB") 

import locale
locale.setlocale( locale.LC_ALL, '' )

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.limit_order import LimitOrder
from cryptomancer.execution_handler.auto_limit_order import AutoLimitOrder

from typing import Tuple

def patient_entry(account_name: str, base: str, underlying: str, dollar_target: float, 
                    side: str, timeout: float, min_trade_size: float) -> Tuple[float, float]:
    """
    This function seeks to buy/sell (`side`) `dollar_target` of `underlying` with posted limit orders to
        avoid taker fees.  The trade is for account `account_name`.
    
        - Uses current bid/ask spread to create the posted limit order and then wait `timeout` seconds before cancelling.
        - If after 5 retries no fills have occurred, attempts a limit order _above_ the mid-point.
    """

    account = FtxAccount(account_name)
    exchange_feed = FtxExchangeFeed(account_name)

    logger.debug(f'{base} | Subscribing to market feed.')
    _ = exchange_feed.get_ticker(underlying)
    # need to let the subscription go through before we proceed
    while True:
        market = exchange_feed.get_ticker(underlying)
        # make sure it's not an empty dict
        # could also do `if not market`, but that
        # seems like ugly code
        if len(market) != 0:
            break
        time.sleep(0.1)
        
    logger.debug(f'{base} | Subscribed.')

    fills = []
    fill_prices = []

    for retry in range(6):
        # GET CURRENT MID-POINT TO FIGURE OUT SIZE
        market = exchange_feed.get_ticker(underlying)
        mid_point = (market['bid'] + market['ask']) / 2.
        width = (market['ask'] - market['bid']) / mid_point
    
        size = dollar_target / mid_point
        limit_price = mid_point * (1 - width / 2) if side == 'buy' else mid_point * (1 + width / 2)
        
        if retry < 5:
            logger.debug(f'{base} | Attempt #{retry + 1} at providing liquidity @ '
                         f'{locale.currency(limit_price, grouping = True)}...')
            try:
                with execution_scope(wait = True, timeout = timeout) as session:
                    underlying_order = LimitOrder(account = account,
                                                    market = underlying,
                                                    side = side,
                                                    size = size,
                                                    price = limit_price,
                                                    post_only = True)
                    session.add(underlying_order)
            
            except TimeoutError:
                logger.debug(f'{base} | Attempt #{retry + 1} timed out.')
                # we pass instead of continue here because there is a chance 
                # we actually got a partial fill before our cancel got in
                pass

        else:
            logger.info(f'{base} | Failed {retry} times; now trying to take liquidity...')
            break

            try:
                with execution_scope(wait = True) as session:
                    underlying_order = AutoLimitOrder(account = account,
                                                exchange_feed = exchange_feed,
                                                market = underlying,
                                                side = side,
                                                size = size,
                                                width = width)
                    session.add(underlying_order)
            except:
                continue
            
        # MAKE SURE THE TRADE ACTUALLY WENT THROUGH
        order_status = session.get_order_statuses()
        if len(order_status) == 0:
            # THERE MUST'VE BEEN AN ERROR WITH THE TRADE
            logger.info(f'{base} | {side.upper()} {size:,.4f} {underlying} FAILED')
            continue

        else:
            order_status = order_status[0]

            while order_status.order_id != -1 and order_status.status == "open":
                logger.info(f'{base} | Timed out, but waiting for order to cancel...')
                try:
                    ftx_account.cancel_order(order_status.order_id)
                except:
                    pass
                time.sleep(0.1)
                order_status = session.get_order_statuses()[0]

            # see if we got any fills.
            filled_size = order_status.filled_size

            if abs(filled_size) < 1e-8:
                if order_status.order_id == -1:
                    logger.info(f'{base} | Attemped {side.upper()} {size:,.4f} | Exception: {order_status.exception}')
                continue

            fill_price = order_status.average_fill_price
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
    
    # the whole loop completed without breaking.
    else:
        order_status = session.get_order_statuses()
        if len(order_status) == 0:
            logger.info(f'{base} | Failed to execute entry order: No Information.  Bailing...')
        else:
            order_status = order_status[0]
            logger.info(f'{base} | Failed to execute entry order: {order_status.exception}.  Bailing...')

        return (fills, fill_prices)

    return (fills, fill_prices)
