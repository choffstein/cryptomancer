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

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed
from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.market_order import MarketOrder
from cryptomancer.execution_handler.limit_order import LimitOrder
from cryptomancer.execution_handler.auto_limit_order import AutoLimitOrder


from entry_models.patient_entry import patient_entry

from exit_models.trailing_stop import trailing_stop
from exit_models.take_profit import take_profit


def run(base, proxy, account_name, dollar_target, volatility, min_trade_size):

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
    
    # SLEEP UNTIL READY TO RUN
    now = pytz.utc.localize(datetime.datetime.utcnow())
    if now.hour == 0:
        tomorrow = now
    else:
        tomorrow = now + datetime.timedelta(days = 1)

    # REBAL TIME IS AT 00:02:00 UTC
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 0, 0))

    # Try to TWAP the trade 10 times over the 5 minutes prior to the rebalance target time
    # (each TWAP is given 30s; if we run the program with less than 5 minutes until rebalance
    #  we'll end up doing fewer rebals, but we keep the 30s window)
    delta_s = 30
    n_rebals = max(1, min(10, int((rebal_time - now).seconds / delta_s)))
    dollars_per_rebal = dollar_target / n_rebals

    market = exchange_feed.get_ticker(underlying)
    mid_point = (market['bid'] + market['ask']) / 2.

    while (dollars_per_rebal / mid_point) < min_trade_size and n_rebals > 0:
        n_rebals = n_rebals - 1
        dollars_per_rebal = dollar_target / n_rebals

    # JUST 1 BUY
    n_rebals = 1 
    dollars_per_rebal = dollar_target

    if n_rebals == 0:
        logger.info(f'{base} | Dollar trade target too small for a single rebalance.  Exiting.')
        return

    logger.info(f'{base} | TWAPing with {n_rebals} orders.')

    # set the TWAP times
    # for testing we can use "now +", but in actual practice we want to use "rebal_time -"
    #execution_times = [rebal_time - datetime.timdelta(seconds = 30 * i) for i in range(0, n_rebals)]
    execution_times = [now + datetime.timedelta(seconds = 5 * i) for i in range(0, n_rebals)]

    # need to force this to a list because zip iterable doesn't work with 
    # parallel code
    args = zip(list(range(1, n_rebals+1)),
                [account_name] * n_rebals,
                [base] * n_rebals,
                [underlying] * n_rebals,
                [dollars_per_rebal] * n_rebals,
                execution_times,
                [side] * n_rebals,
                [5] * n_rebals,
                [min_trade_size] * n_rebals)

    # run the TWAP as parallel threads
    results = cryptomancer.parallel.lmap(patient_entry, args)

    # calculate the results as total fill and average fill price
    fills = [result[0] for result in results] 
    fill_prices = [result[1] for result in results]

    fills = [fill for sublist in fills for fill in sublist]
    fill_prices = [fill_price for sublist in fill_prices for fill_price in sublist]

    total_fill = numpy.sum(fills)
    average_fill_price = numpy.dot(fills, fill_prices) / numpy.sum(fills)

    # WE HAVE TO GO BACK TO SLEEP HERE JUST IN CASE WE GO FILLED TOO EARLY; WE
    # DON'T WANT OUR STOP LOSS TRIGGERING TOO EARLY
    now = pytz.utc.localize(datetime.datetime.utcnow())
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 30, 0))
    time_until_rebalance = rebal_time.timestamp() - now.timestamp()

    logger.info(f"{base} | Sleeping for {time_until_rebalance:,.2f}s...")
    #if time_until_rebalance > 0:
    #    time.sleep(time_until_rebalance)
    logger.info(f"{base} | Awake and ready to put on the stop!")

    # WITH THE FILLED SIZE, SET A TRAILING STOP SO WE CAN
    # TRY TO BENEFIT FROM ANY MOMENTUM THAT OCCURS
    # THIS CODE IS LIQUIDITY TAKING, BUT LIKELY CHEAPER
    # THAN TRYING TO CHASE MARKETS WITH POST ORDERS
    size = abs(total_fill)
    side = 'sell' if underlying_to_rebal > 1e-8 else 'buy'

    # sell 1/3rd of size at a 2x vol trigger
    # sell 1/3rd of size at 3x vol trigger
    # sell all size with 1x vol trigger

    # if we need the take profit to buy, we want it to be above the current price
    # if the take profit is a sell, we want it below the current price
    take_profit_l1 = average_fill_price * (1. - 2 * volatility) if side == 'buy' else average_fill_price * (1. + 2 * volatility)
    take_profit_l1_args = (account_name, base, underlying, size / 3, side, take_profit_l1)

    take_profit_l2 = average_fill_price * (1. - 3 * volatility) if side == 'buy' else average_fill_price * (1. + 3 * volatility)
    take_profit_l2_args = (account_name, base, underlying, size / 3, side, take_profit_l2)

    trail_value = average_fill_price * volatility
    trailing_stop_args = (account_name, base, underlying, size, side, trail_value)

    cryptomancer.parallel.aync_run([(take_profit, take_profit_l1_args),
                                    (take_profit, take_profit_l2_args),
                                    (trailing_stop, trailing_stop_args)]


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Dollar Target>"
    parser = OptionParser(usage = usage)
    (options, args) = parser.parse_args()

    if len(args) < 2:
        print(usage)
        exit()

    account_name = args[0]
    dollar_target = float(args[1])

    # do the import here to avoid re-importing with the parallel calls
    from cryptomancer.security_master import SecurityMaster
    sm = SecurityMaster("FTX")

    min_size = {}
    min_price_increment = {}
    for underlying in ['BTC', 'ETH', 'DOGE', 'MATIC', 'ADA', 'SOL','XRP']:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']

    # replace this with volatility
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
    
    #run(*parameters[1])

    cryptomancer.parallel.lmap(run, parameters)
