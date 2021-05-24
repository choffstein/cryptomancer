from typing import Optional
import time
import datetime
import numpy

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.exchange_feed import ExchangeFeed
from cryptomancer.account import Account


class AutoLimitOrder(Order):
    def __init__(self, account: Account, exchange_feed: ExchangeFeed, 
                market: str, side: str, size: float, timeout: float = 1):
        super().__init__(account, exchange_feed)
        self._market = market
        self._side = side
        self._size = size

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        exchange_feed = self.get_exchange_feed()
        account = self.get_account()

        filled_size = 0
        error = False

        while not numpy.isclose(filled_size, self._size) and not error:
            if self.get_id():
                # if we have a current trade going, we need to cancel it
                self.cancel()
                self.wait_until_closed()
                
                # if we got any fills, we need to update our filled size
                status = self.get_status()
                filled_size = filled_size + status.filled_size

                if not numpy.isclose(filled_size, self._size):
                    # make sure the status filled size reflects the full fill status
                    status.filled_size = self._size
                    continue

            # get the current orderbook
            order_book = exchange_feed.cumulative_orderbook(self._market)
            if self._side == 'buy':
                asks = order_book['asks']
                prices = [ask[0] for ask in asks]
                depths = [ask[1] for ask in asks]

            else:
                bids = order_book['bids']
                prices = [bid[0] for bid in bids]
                depths = [bid[1] for bid in bids]

            # figure out what level we'd eat through if we just spammed a market order
            level = numpy.sum(~(self._size < numpy.array(depths)))
            max_impact_px = prices[level]
            top = prices[0]

            # set limit price as half way between top and max_impact_px
            limit_price = (top + max_impact_px) / 2.

            try:
                # assuming we still have more to fill, enter a new limit order
                status = account.place_order(market = self._market, side = self._side, price = limit_price, 
                                size = self._size - filled_size, type = "limit", ioc = False)
                self.set_id(status.order_id)
            
                # Zzz...
                time.sleep(timeout)

                # get the order status; see if we're closed or not
                status = self.get_status()
                if status.status == "closed":
                    filled_size = filled_size + status.filled_size

            except:
                status = OrderStatus(order_id = -1,
                                created_time = datetime.datetime.utcnow(),
                                market = self._market,
                                side = self._side,
                                size = self._size,
                                filled_size = filled_size,
                                status = "closed"
                )
                error = True

        return status


    @session_required
    def rollback(self):
        if not self.get_id():
            return

        try:
            self.cancel()
            self.wait_until_closed()
        except:
            pass            

        order_status = self.get_status()
        filled = order_status.filled_size

        if filled > 1e-8:
            side = "buy" if self._side == "sell" else "sell"
            account = self.get_account()
            status = account.place_order(market = self._market, side = side, price = None, 
                                    size = filled, type = "market", ioc = True)
            self.set_id(status.order_id)

            self.wait_until_closed()
