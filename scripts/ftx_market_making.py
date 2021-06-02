import sys
from optparse import OptionParser

from typing import Optional, Tuple
import time

from loguru import logger
logger.add("logs/ftx_market_making.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed

import numpy
import tqdm


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Underlying> <Size> [optional-args]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("-c", "--calibration-period",
                      help="EWMA span to calibrate volatility to", type=int, dest="ewma_span", default = 120)
    parser.add_option("-g", "--risk-aversion",
                       help="Inventory risk aversion parameter", type=float, dest="gamma", default = 0.5)
    parser.add_option("-t", "--horizon",
                       help="Horizon of the trade; defaults to infinite", type=float, dest="T", default = numpy.inf)
    parser.add_option("-k", "--liquidity",
                       help="Orderbook liquidity parameter", type=float, dest="k", default = 1.5)
    parser.add_option("-q", "--qmax",
                        help = "Multiplier on size that defines maximum inventory", type = float, dest="q_max", default = 5)                 

    (options, args) = parser.parse_args()
    if len(args) < 3:
        print(usage)
        sys.exit(1)

    account_name = args[0]
    underlying = args[1].upper()
    size = float(args[2])
    q_max = size * options.q_max

    ftx_account = FtxAccount(account_name)
    ftx_feed = FtxExchangeFeed(account_name)

    #subscribe
    for retries in range(3):
        market = ftx_feed.get_ticker(underlying)
        if 'bid' in market.keys():
            break
        time.sleep(1)
    else:
        raise Exception("Couldn't create exchange feed")
        

    logger.info(f'Calibrating initial volatility...')
    variance = 0
    alpha = 2. / (1. + options.ewma_span)
    market = ftx_feed.get_ticker(underlying)
    prior_mid = (market['bid'] + market['ask']) / 2
    for i in tqdm.tqdm(range(options.ewma_span)):
        try:
            market = ftx_feed.get_ticker(underlying)
            mid = (market['bid'] + market['ask']) / 2
            variance = (1 - alpha) * variance + alpha * (60 * 60 * 24 * 365) * (mid / prior_mid - 1)**2
            prior_mid = mid
            time.sleep(1)
        except:
            continue

    
    dt = (1 / 60) * (1 / 60) * (1 / 24)
    n = 0
    while True:
        positions = ftx_account.get_positions()
        underlying_positions = list(filter(lambda position: position.name.upper() == underlying, positions))

        underlying_position_size = sum([position.net_size for position in underlying_positions])
        underlying_position_dollars = sum([position.usd_value for position in underlying_positions])

        total_portfolio_value = sum([abs(position.usd_value) for position in positions])

        q = underlying_position_size

        logger.info(f'Current Exposure: {underlying_position_size} | ${underlying_position_dollars}')

        try:
            market = ftx_feed.get_ticker(underlying)
            mid = (market['bid'] + market['ask']) / 2
            variance = (1 - alpha) * variance + alpha * (60 * 60 * 24 * 365) * (mid / prior_mid - 1)**2
            prior_mid = mid
        except:
            continue

        import ipdb; ipdb.set_trace()

        # this is all based upon Avellaneda-Stoikov model
        if numpy.isinf(options.T):
            w = 0.5 * options.gamma**2 * variance * (q_max + 1)**2
            coef = options.gamma**2 * variance / (2*w - options.gamma**2 * q**2 * variance)
            r_ask = mid + (1 / options.gamma) * numpy.log(1 + ( 1 - 2*q) * coef)
            r_bid = mid + (1 / options.gamma) * numpy.log(1 + (-1 - 2*q) * coef)
            reservation_price = (r_ask + r_bid) / 2
        else:
            reservation_price = s - q * options.gamma * variance * (T - n * dt)
            r_spread = 2 / options.gamma * numpy.log(1 + options.gamma / options.k)
            r_ask = reservation_price + r_spread / 2.
            r_bid = reservation_price - r_spread / 2.

        px = {
            'buy': r_bid,
            'sell': r_ask
        }

        import ipdb; ipdb.set_trace()

        logger.info(f'Current Exposure: {underlying_position_size} | ${underlying_position_dollars}')

        # WHAT ORDER WE POST SHOULD BE BASED UPON MARKET TREND?
        #   BUY AND SELL AT THE SAME TIME FOR FLAT MARKET?
        #   BUY FIRST, THEN SELL FOR UP-TRENDING
        #   SELL FIRST, THEN BUY FOR DOWN-TRENDING

        # WHAT'S OUR MINIMUM SPREAD?
        """
        bid_order = None
        try:
            bid_order = ftx_account.place_order(underlying, side = 'buy', price = px['buy'], size = size, post_only = True)
            logger.info(f'Bid Order: {bid_order.order_id}')
        except:
            pass
            

        ask_order = None
        try:
            ask_order = ftx_account.place_order(underlying, side = 'sell', price = px['sell'], size = size, post_only = True)
            logger.info(f'Ask Order: {ask_order.order_id}')
        except:
            pass
            
        time.sleep(1)

        if bid_order:
            try:
                ftx_account.cancel_order(bid_order.order_id)
            except:
                pass

        if ask_order:
            try:
                ftx_account.cancel_order(ask_order.order_id)
            except:
                pass
        """

        time.sleep(1)

        n = n * 1