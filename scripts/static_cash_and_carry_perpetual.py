import sys
from optparse import OptionParser

from typing import Optional, Tuple
import time

from loguru import logger
logger.add("logs/ftx_static_cash_and_carry_perpetual.log", rotation="100 MB") 

from cryptomancer.security_master import SecurityMaster

from cryptomancer.account.ftx_account import FtxAccount
from cryptomancer.exchange_feed.ftx_exchange_feed import FtxExchangeFeed

from cryptomancer.execution_handler.execution_session import execution_scope
from cryptomancer.execution_handler.market_order_dollars import MarketOrderDollars
from cryptomancer.execution_handler.market_order import MarketOrder


def static_cash_and_carry(account: Account, exchange_feed: ExchangeFeed, underlying: str, 
                        cash_collateral_target: float, cash_collateral_bounds: Tuple[float, float], 
                        minimum_size: Optional[float] = 0.001):
    underlying_name = f'{underlying}/USD'
    future_name = f'{underlying}-PERP'
    
    # get the current positions
    positions = account.get_positions()

    for position in positions:
        logger.info(f'Current Position | {position.name} | {position.side} | {position.size} | ${position.usd_value}')
    
    usd_coins = list(filter(lambda position: position.name == 'USD', positions))
    underlying_positions = list(filter(lambda position: position.name.upper() == underlying, positions))
    perpetual_positions = list(filter(lambda position: position.name.upper() == future_name, positions))

    # figure out how much cash collateral we have
    # we have to be careful here because USD will already *include* PERP gains/losses,
    # 	so we don't want to double-count those values
    portfolio_value = sum([position.usd_value for position in usd_coins]) + \
                        sum([position.usd_value for position in underlying_positions])

    logger.info(f'Current Portfolio Value | ${portfolio_value}')

    usd_value = sum([position.usd_value for position in usd_coins])
    underlying_size = sum([position.net_size for position in underlying_positions])
    perpetual_size = sum([position.net_size for position in perpetual_positions])

    margin_pct = usd_value / portfolio_value

    logger.info(f'Current Margin | ' + '{:.2%}'.format(margin_pct))

    # if we're within our collateral bounds, do nothing
    if margin_pct > cash_collateral_bounds[0] and margin_pct < cash_collateral_bounds[1]:
        return

    logger.info('Margin Target Out of Bounds (' + '{:.2%}'.format(cash_collateral_bounds[0]) + ', ' + 
                                                '{:.2%}'.format(cash_collateral_bounds[1]) + ')')

    # otherwise, figure out what our target positions are
    target_margin_usd = portfolio_value * cash_collateral_target
    target_exposure_usd = portfolio_value * (1 - cash_collateral_target)

    current_exposure_usd = sum([position.usd_value for position in underlying_positions])

    target_usd_trade = target_exposure_usd - current_exposure_usd

    # we do the trade in the underlying first because we're trying to hit a specific
    # dollar amount; once we get that dollar amount executed, we can match it with
    # a corresponding trade in the perpetuals
    with execution_scope() as session:
        side = 'buy' if target_usd_trade > 1e-8 else 'sell'

        target_usd_trade = abs(target_usd_trade)
        logger.info(f'{side.upper()} ${target_usd_trade} {underlying_name}')
        underlying_order = MarketOrderDollars(account = account,
                                                exchange_feed = exchange_feed,
                                                market = underlying_name,
                                                side = side,
                                                size_usd = target_usd_trade)

        session.add(underlying_order)

    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # the order errored out
        logger.info(f'{side.upper()} ${target_usd_trade} {underlying_name} FAILED')
        return
    else:
        order_status = order_status[0]

    # figure out how much of the order was actually filled
    filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
    total_underlying = sum([position.net_size for position in underlying_positions])
    total_underlying = total_underlying + filled_size

    logger.info(f'Filled {filled_size} in {underlying_name} | Total: {total_underlying}')

    # if we have 5 underlying, we need -5 perpetuals
    # so we want to take that target and subtract what we already own
    perpetual_to_buy = (-total_underlying - perpetual_size)
    perpetual_to_buy = int(perpetual_to_buy / minimum_size) * minimum_size

    with execution_scope() as session:
        side = 'buy' if perpetual_to_buy > 1e-8 else 'sell'
        size = abs(perpetual_to_buy)
        logger.info(f"{side.upper()} {size} {future_name}")
        perpetual_order = MarketOrder(account = account,
                            market = future_name,
                            side = side,
                            size = size)
    
        session.add(perpetual_order)

    order_status = session.get_order_statuses()
    if len(order_status) == 0:
        # the order errored out
        logger.info(f'{side.upper()} {size} {future_name} FAILED')
        return
    else:
        order_status = order_status[0]

    filled_size = order_status.filled_size if order_status.side == "buy" else -order_status.filled_size
    total_perpetual = sum([position.net_size for position in perpetual_positions])
    total_perpetual = total_perpetual + filled_size

    logger.info(f'Filled {filled_size} in {future_name} | Total: {total_perpetual}')


if __name__ == '__main__':
    usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Underlying> [optional-args]"
    parser = OptionParser(usage = usage)
    
    parser.add_option("-m", "--margin",
                      help="Margin target", type=float, dest="margin", default=0.2)
    parser.add_option("-l", "--margin-lower",
                      help="Lower bound threshold for margin (forced rebalance)", type=float, dest="margin_low", default=0.15)
    parser.add_option("-u", "--margin-upper",
                      help="Upper bound threshold for margin (forced rebalance)", type=float, dest="margin_high", default=0.25)                                

    (options, args) = parser.parse_args()
    if len(args) < 2:
        print(usage)
        sys.exit(1)

    if options.margin < options.margin_low:
        print("Margin target must be higher than lower margin threshold.")
        sys.exit(0)

    if options.margin > options.margin_high:
        print("Margin target must be lower than upper margin threshold.")
        sys.exit(0)

    if options.margin < 0 or options.margin > 1:
        print("Margin target must be between 0-100%.")
        sys.exit(0)

    if options.margin_low < 0:
        print("Margin lower bound must be greater than 0%.")
        sys.exit(0)

    if options.margin_high > 1:
        print("Margin upper bound must be less than 100%.")
        sys.exit(0)

    if options.sleep < 0:
        print("Sleep time must be greater than 0.")
        sys.exit(0)

    account_name = args[0]
    underlying = args[1].upper()

    args = parser.parse_args()
    
    sm = SecurityMaster("FTX")
    try:
        sm.get_market_spec(underlying + "/USD")
    except:
        logger.exception("Invalid underlying (does not exist in Securities Master database)")
        sys.exit(0)

    try:
        sm.get_contract_spec(underlying + '-PERP')
    except:
        logger.exception("No associated perpetual contract at FTX (does not exist in Securities Master database)")
        sys.exit(0)

    try:
        ftx_account = FtxAccount(account_name)
        ftx_feed = FtxExchangeFeed(account_name)
    except Exception as e:
        logger.exception(e)
        sys.exit(0)

    static_cash_and_carry(ftx_account, ftx_feed, underlying, 
                            options.margin, (options.margin_low, options.margin_high))
