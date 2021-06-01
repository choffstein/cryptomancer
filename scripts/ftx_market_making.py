import sys
from optparse import OptionParser

from typing import Optional, Tuple
import time

from loguru import logger
logger.add("logs/ftx_market_making.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Underlying> <Size> [optional-args]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("-l", "--delta-lower",
                      help="Lower bound for delta exposure", type=float, dest="delta_low", default=-0.5)
    parser.add_option("-u", "--delta-upper",
                      help="Upper bound for delta exposure", type=float, dest="delta_high", default=0.5)                                

    (options, args) = parser.parse_args()
    if len(args) < 3:
        print(usage)
        sys.exit(1)

    account_name = args[0]
    underlying = args[1].upper()
    size = float(args[2])

    ftx_account = FtxAccount(account_name)
    ftx_feed = FtxExchangeFeed(account_name)
 
    while True:
        positions = ftx_account.get_positions()
        underlying_positions = list(filter(lambda position: position.name.upper() == underlying, positions))

        underlying_position_size = sum([position.net_size for position in underlying_positions])
        underlying_position_dollars = sum([position.usd_value for position in underlying_positions])

        logger.info(f'Current Exposure: {underlying_position_size} | ${underlying_position_dollars}')

        try:
            market = ftx_feed.get_ticker(underlying)
            mid = (market['bid'] + market['ask']) / 2
        except:
            continue

        width = 0.001
        px = {
            'buy': mid * (1. - width / 2),
            'sell': mid * (1. + width / 2)
        }

        bid_order = None
        if underlying_position_size < options.delta_high:
            try:
                bid_order = ftx_account.place_order(underlying, side = 'buy', price = px['buy'], size = size, post_only = True)
                logger.info(f'Bid Order: {bid_order.order_id}')
            except:
                pass
            

        ask_order = None
        if underlying_position_size > options.delta_low:
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
