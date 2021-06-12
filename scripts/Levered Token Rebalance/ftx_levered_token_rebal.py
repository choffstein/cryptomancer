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

    # SUBSCRIBE TO BID/ASK FEEDS + TRADES
    _ = exchange_feed.get_ticker(underlying)

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
    
    n_rebals = int(underlying_to_rebal / 4000000) + 1

    logger.info(f'{base} | Expected Rebalance of {proxy}-PERP: '
                f'{locale.currency(nav_to_rebal, grouping = True)} / {underlying_to_rebal:,.2f} shares / {n_rebals} trades')

    side = 'buy' if underlying_to_rebal > 1e-8 else 'sell'
    
    # SLEEP UNTIL READY TO RUN
    now = pytz.utc.localize(datetime.datetime.utcnow())
    if now.hour == 0:
        tomorrow = now
    else:
        tomorrow = now + datetime.timedelta(days = 1)

    
    # REBAL TIME IS AT 00:02:00 UTC
    # THERE IS WEIRD, ABNORMALLY POSITIVE VOLUME AT 00:00.  IF WE'RE BUYING, BUY AT 23:59:30
    # IF WE ARE SELLING, SELL AT 00:01:30
    rebal_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 0, 0))
    #if side == 'buy':
    #    execution_time = rebal_time - datetime.timedelta(seconds = 30*5)
    #else:
    execution_time = rebal_time - datetime.timedelta(seconds = 30)
    
    # do a patient entry 
    time_until_entry = execution_time.timestamp() - now.timestamp()
    if time_until_entry > 0:
        logger.info(f"{base} | Sleeping for {time_until_entry:,.2f}s...")
        time.sleep(time_until_entry)
        logger.info(f"{base} | Awake and ready to put on trade!")

    fills, fill_prices = patient_entry(account_name, base, underlying, dollar_target, side, 5, min_trade_size)

    total_fill = numpy.sum(fills)
    average_fill_price = numpy.dot(fills, fill_prices) / numpy.sum(fills)

    # WE HAVE TO GO BACK TO SLEEP HERE JUST IN CASE WE GO FILLED TOO EARLY; WE
    # DON'T WANT OUR STOP LOSS TRIGGERING TOO EARLY
    end_time = pytz.utc.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 2, 0, 0))
    # 20 seconds for the first two rebalances, 11 seconds for every rebalance after that
    end_time = end_time + min(n_rebals, 2) * datetime.timedelta(seconds = 20) + max(0, (n_rebals - 2)) * datetime.timedelta(seconds = 11)

    now = pytz.utc.localize(datetime.datetime.utcnow())
    time_until_end = end_time.timestamp() - now.timestamp()

    if time_until_end > 0:
        logger.info(f"{base} | Sleeping for {time_until_end:,.2f}s...")
        time.sleep(time_until_end)
        logger.info(f"{base} | Awake and ready to put on the stop!")


    # WITH THE FILLED SIZE, SET A TRAILING STOP SO WE CAN
    # TRY TO BENEFIT FROM ANY MOMENTUM THAT OCCURS
    # THIS CODE IS LIQUIDITY TAKING, BUT LIKELY CHEAPER
    # THAN TRYING TO CHASE MARKETS WITH POST ORDERS
    size = abs(total_fill)
    side = 'sell' if underlying_to_rebal > 1e-8 else 'buy'

    market = ftx_client.get_market(underlying)
    mid_point = (market['bid'] + market['ask']) / 2.
    trail_value = mid_point * volatility

    # stop out of our position and reverse with half the size
    trailing_stop(account_name, base, underlying, size * 1.5, side, trail_value, reduce_only = False)

    # now set another trailing stop for half the size
    side = 'buy' if side == 'sell' else 'sell'
    trailing_stop(account_name, base, underlying, size * 0.5, side, trail_value, reduce_only = True)



if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Dollar Target>"
    parser = OptionParser(usage = usage)
    (options, args) = parser.parse_args()

    if len(args) < 1:
        print(usage)
        exit()

    account_name = args[0]
    #dollar_target = float(args[1])

    # do the import here to avoid re-importing with the parallel calls
    from cryptomancer.security_master import SecurityMaster
    sm = SecurityMaster("FTX")

    to_trade = ['BTC', 'ETH', 'ADA', 'XRP'] #'DOGE', 'MATIC', 'ADA', 'SOL', 'XRP']

    min_size = {}
    min_price_increment = {}
    for underlying in to_trade:
        spec = sm.get_contract_spec(underlying + '-PERP')
        min_size[underlying] = spec['sizeIncrement']
        min_price_increment[underlying] = spec['priceIncrement']

    now = pytz.utc.localize(datetime.datetime.utcnow())
    yesterday = now - datetime.timedelta(days = 1)

    end_ts = int(now.timestamp())
    start_ts = int(yesterday.timestamp())

    # get 1-day 1-minute bar volatility levels
    import ftx
    ftx_client = ftx.FtxClient()

    vol = {}
    for underlying in to_trade:
        df = ftx_client.get_historical_data(underlying + '-PERP', resolution = 60, limit = None, start_time = start_ts, end_time = end_ts)
        df = pandas.DataFrame(df).set_index('startTime').sort_index()['close']
        vol[underlying] = df.apply(numpy.log).diff().std()

    proxy = {
        'BTC': 'BTC',
        'ETH': 'ETH',
        #'DOGE': 'DOGE',
        #'MATIC': 'MATIC',
        'ADA': 'ETH',
        #'SOL': 'SOL',
        'XRP': 'ETH'
    }

    dollar_targets = {
        'BTC': 5000,
        'ETH': 5000,
        'ADA': 2500,
        'XRP': 2500
        #'DOGE': 1500,
        #'MATIC': 1500,
        #'ADA': 1500,
        #'SOL': 1500,
        #'XRP': 1500
    }

    parameters = []
    for underlying in to_trade:
        parameters.append((underlying, proxy[underlying], 
                                account_name, dollar_targets[underlying], 
                                vol[underlying], min_size[underlying]))
    
    #run(*parameters[1])
    cryptomancer.parallel.lmap(run, parameters)
