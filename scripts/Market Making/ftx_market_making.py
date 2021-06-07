##########################
#
# Implements the Avellaneda-Stoikov model
#   - adjustments for maximum / minimum spread values
#   - adjustments for significant changes in volatility
#   - bid/ask shape a la Optimal High-Frequency Market Making (Fushimi, Gonzalez Rojas, and Herma 2018)
##########################

import sys
from optparse import OptionParser

from typing import Optional, Tuple
import datetime
import time

from loguru import logger
logger.add("logs/ftx_market_making.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed

from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.limit_order import LimitOrder

import numpy
import tqdm

import vectorized


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Underlying> <Size> [optional-args]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("-c", "--calibration-period",
                      help="EWMA span to calibrate volatility to (# of trades)", type=int, dest="ewma_span", default = 120)
    parser.add_option("-g", "--risk-aversion",
                       help="Inventory risk aversion parameter in (0, inf)", type=float, dest="gamma", default = 0.5) #0 is no risk, 1 is very risky
    parser.add_option("-t", "--horizon",
                       help="Horizon of the trade; defaults to 8h", type=float, dest="T", default = 60*60*8)
    parser.add_option("-k", "--liquidity",
                       help="Orderbook liquidity parameter", type=float, dest="k", default = 1.5)
    parser.add_option("-v", "--volatility-spread",
                       help="Volatility spread multipler", type=float, dest="vol_to_spread_multiplier", default = 1.3)
    parser.add_option("-m", "--min-spread",
                       help="Minimum spread size", type=float, dest="min_spread", default = 0.001)
    parser.add_option("-x", "--max-spread",
                       help="Maximum spread size", type=float, dest="max_spread", default = 0.01)
    parser.add_option("-s", "--sleep",
                       help="Time to sleep between orders", type=float, dest="sleep", default = 5)


    (options, args) = parser.parse_args()
    if len(args) < 3:
        print(usage)
        sys.exit(1)

    account_name = args[0]
    underlying = args[1].upper()
    size = float(args[2])
    
    inventory_target_base_pct = 0

    # Based upon (Fushimi, Gonzalez Rojas, and Herma 2018)
    # In the paper, default size was 100 and n = -0.005
    #    if size is smaller, we need n to be larger
    #    and if size is larger, we need n to be smaller
    size_shape = (100 / size) * -0.005


    try:
        split = underlying.split("/")
        base_asset = split[0]
        quote_asset = split[1]
    except:
        base_asset = underlying
        quote_asset = 'USD'

    MAX_LIST_SIZE = options.ewma_span * 4

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
    
    ewma_alpha = 2. / (options.ewma_span + 1)

    logger.info(f'Calibrating initial volatility...')
    trades = []
    pbar = tqdm.tqdm(total = options.ewma_span)
    while len(trades) < options.ewma_span:
        new_trades = ftx_feed.get_trades(underlying)
        new_trades = [trade['price'] for trade in new_trades]
        trades = trades + new_trades
        pbar.update(len(new_trades))
    

    start_time = datetime.datetime.now()
    closing_time = start_time + datetime.timedelta(seconds = options.T)
    time_left_fraction = (closing_time - datetime.datetime.now()) / (closing_time - start_time)

    while time_left_fraction > 0:
        positions = ftx_account.get_positions()
        quote_positions = list(filter(lambda position: position.name == quote_asset, positions))
        quote_asset_amount = sum([position.net_size for position in quote_positions])

        base_positions = list(filter(lambda position: position.name.upper() == underlying, positions))
        base_asset_amount = sum([position.net_size for position in base_positions])

        logger.info(f'Current Exposure: {base_asset} {base_asset_amount} / {quote_asset} {quote_asset_amount}')

        new_trades = ftx_feed.get_trades(underlying)
        new_trades = [trade['price'] for trade in new_trades]
        trades = trades + new_trades

        if len(trades) > MAX_LIST_SIZE:
            trades = trades[len(trades) - MAX_LIST_SIZE:]

        # get px
        market = ftx_feed.get_ticker(underlying)
        px = (market['bid'] + market['ask']) / 2.
        
        base_value = px * base_asset_amount
        inventory_value = base_value + quote_asset_amount
        target_inventory_value = inventory_value * inventory_target_base_pct
        target_inventory_size = target_inventory_value / px

        logger.info(f'Target Exposure: {base_asset} {target_inventory_size} / {quote_asset} {target_inventory_value}')

        inventory_in_base = quote_asset_amount / px + base_asset_amount
        q_adjustment_factor = 1e5 / inventory_in_base

        q = (base_asset_amount - target_inventory_size) * q_adjustment_factor

        # we only take the diff, not the log diff... not annualizing here, but getting 
        # the local price variance
        variance = vectorized.ewma(numpy.square(numpy.diff(trades)), ewma_alpha)

        variance = variance[-1]
        volatility = numpy.sqrt(variance)

        time_left_fraction = (closing_time - datetime.datetime.now()) / (closing_time - start_time)

        reservation_price = px - q * options.gamma * variance * time_left_fraction
        optimal_spread = 2 / options.gamma * numpy.log(1 + options.gamma / options.k)

        spread_inflation_due_to_volatility = max(options.vol_to_spread_multiplier * volatility, px * options.min_spread) / (px * options.min_spread)

        min_limit_bid = px * (1. - options.max_spread * spread_inflation_due_to_volatility)
        max_limit_bid = px * (1. - options.min_spread * spread_inflation_due_to_volatility)
        min_limit_ask = px * (1. + options.min_spread * spread_inflation_due_to_volatility)
        max_limit_ask = px * (1. + options.max_spread * spread_inflation_due_to_volatility)

        r_ask = min(max(reservation_price + optimal_spread / 2.,
                            min_limit_ask),
                        max_limit_ask)

        r_bid = min(max(reservation_price - optimal_spread / 2.,
                            min_limit_bid),
                        max_limit_bid)

        size_bid = size_ask = 0
        q_unadj = q / q_adjustment_factor
        
        with execution_scope(wait = False) as session:
            if r_bid < px:
                size_bid = size if q_unadj < 0 else size * numpy.exp(size_shape * q_unadj)

                buy_order = LimitOrder(account = ftx_account,
                                        market = underlying,
                                        side = "buy",
                                        size = size_bid,
                                        price = r_bid,
                                        post_only = True)
                session.add(buy_order)

            if r_ask > px:
                size_ask = size if q_unadj > 0 else size * numpy.exp(-size_shape * q_unadj)
                sell_order = LimitOrder(account = ftx_account,
                                        market = underlying,
                                        side = "sell",
                                        size = size_ask,
                                        price = r_ask,
                                        post_only = True)
                session.add(sell_order)

        logger.info(f'Offers: ({size_bid:.4f}){r_bid:.2f} {px:.2f} {r_ask:.2f}({size_ask:.4f})')

        try:
            logger.info(f'Initiated buy order #{buy_order.get_id()}')
        except:
            pass

        try:
            logger.info(f'Initiated sell order #{sell_order.get_id()}')
        except:
            pass

        ### NOW SLEEP
        logger.info(f'Going to sleep for {options.sleep}s...')
        time.sleep(options.sleep)
        logger.info(f'Awake!')

        ### If we have orders, cancel them

        try:
            order_status = buy_order.get_status()
            if order_status.status == "closed":
                logger.info(f'Buy order #{buy_order.get_id()} filled')
            else:
                logger.info(f'Canceling buy order #{buy_order.get_id()}')
                buy_order.cancel()
        except:
            pass

        try:
            order_status = sell_order.get_status()
            if order_status.status == "closed":
                logger.info(f'Sell order #{sell_order.get_id()} filled')
            else:
                logger.info(f'Canceling sell order #{sell_order.get_id()}')
                sell_order.cancel()
        except:
            pass