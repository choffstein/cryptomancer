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
from cryptomancer.execution_handler.market_order import MarketOrder


class FtxStaticCashAndCarryPerpetual(object):
	def __init__(self, account: FtxAccount, exchange_feed: FtxExchangeFeed, 
				underlying: str, cash_collateral_target: float, 
				cash_collateral_bounds: Tuple[float, float], minimum_size = 0.001):
		
		self._account = account
		self._exchange_feed = exchange_feed

		self._underlying = underlying.upper()
		self._underlying_name = f'{self._underlying}/USD'
		self._future_name = f'{self._underlying}-PERP'
		
		self._cash_collateral_target = cash_collateral_target
		self._cash_collateral_bounds = cash_collateral_bounds
		self._minimum_size = minimum_size
		

	def run(self):
		# get the current positions
		positions = self._account.get_positions()

		# figure out how much cash collateral we have
		total_value = sum([position.usd_value for position in positions])
		usd_value = sum([position.usd_value for position in filter(lambda position: position.name == 'USD', positions)])

		margin_pct = usd_value / total_value

		log.info(f'Current Margin | ' + '{:.2%}'.format(margin_pct))

		# if we're within our collateral bounds, do nothing
		if margin_pct > self._cash_collateral_bounds[0] and margin_pct < self._cash_collateral_bounds[1]:
			return    
	
		logger.info('Margin Target Out of Bounds (' + '{:.2%}'.format(self.cash_collateral_bounds[0]) + ', ' + 
													'{:.2%}'.format(self.cash_collateral_bounds[1]) + ')')

		# otherwise, figure out what our target positions are
		target_margin_usd = total_value * self._cash_collateral_target
		target_exposure_usd = total_value * (1 - self._cash_collateral_target)

		for attempt in range(2):
			try:
				underlying_market = self._exchange_feed.get_current_market(self._underlying_name)
				target_underlying_px = (underlying_market['bid'] + underlying_market['ask']) / 2.
				break

			except:
				logger.info(f"Retrying FTX exchange feed... {attempt + 1}")
				# weird issue where the first time we subscribe to a websocket we can sometimes get
				# a {} response; so probably just retry...
				time.sleep(1)
				continue
		else:
			# we failed all attempts (didn't break from loop)
			logger.exception("Unable to connect to FTX exchange feed.")
			raise Exception("Exchange feed issue")

		# figure out the size of the underlying coin we want to buy
		target_exposure_underlying = target_exposure_usd / target_underlying_px

		# round to minimum size
		target_exposure_underlying = int(target_exposure_underlying / self._minimum_size) * self._minimum_size

		# get the current balance of the underlying in our wallet
		underlying_positions = filter(lambda position: position.name.upper() == self._underlying, positions)
		total_underlying = sum([position.net_size for position in underlying_positions])

		underlying_to_buy = (target_exposure_underlying - total_underlying)

		# get the current net position of the perpetuals
		perpetual_positions = filter(lambda position: position.name.upper() == self._future_name, positions)
		total_perpetual = sum([position.net_size for position in perpetual_positions])

		# if we have 5 underlying, we need -5 perpetuals
		# so we want to take that target and subtract what we already own
		perpetual_to_buy = (-target_exposure_underlying - total_perpetual)

		# generate our orders.  we'll just use market orders here

		with execution_scope() as session:
			if abs(underlying_to_buy) > self._minimum_size:
				side = 'buy' if underlying_to_buy > 1e-8 else 'sell'  

				underlying_order = MarketOrder(account = self._account,
									market = self._underlying_name,
									side = side,
									size = abs(underlying_to_buy))
				#session.add(underlying_order)

			if abs(perpetual_to_buy) > self._minimum_size:
				side = 'buy' if perpetual_to_buy > 1e-8 else 'sell'

				perpetual_order = MarketOrder(account = self._account,
									market = self._future_name,
									side = side,
									size = abs(perpetual_to_buy))
			
				#session.add(perpetual_order)



if __name__ == '__main__':
	usage = "usage: " + sys.argv[0] + " <FTX Account Name> <Underlying> [optional-args]"
	parser = OptionParser(usage = usage)
	parser.add_option("-s", "--sleep",
					  help="How long to sleep (in seconds) between run cycles", type=int, dest="sleep", default=300)
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
		print("Invalid underlying (does not exist in Securities Master database)")
		sys.exit(0)

	try:
		sm.get_contract_spec(underlying + '-PERP')
	except:
		print("No associated perpetual contract at FTX (does not exist in Securities Master database)")
		sys.exit(0)

	ftx_account = FtxAccount(account_name)
	ftx_feed = FtxExchangeFeed(account_name)

	cash_and_carry = StaticCashAndCarryPerpetual(ftx_account, ftx_feed, underlying, 
							options.margin, (options.margin_lower, options.margin_higher))
	
	while True:
		cash_and_carry.run()
		time.sleep(options.sleep)