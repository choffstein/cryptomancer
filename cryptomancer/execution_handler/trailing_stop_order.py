from typing import Optional
import time
import datetime

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.account import Account
from cryptomancer.exchange_feed import ExchangeFeed

class TrailingStopOrder(Order):
    def __init__(self, account: Account, exchange_feed: ExchangeFeed, 
                    market: str, side: str, size: float, 
                    attempts: Optional[int] = 5, width: Optional[float] = 0.001,
                    **kwargs):
        super().__init__('trailing_stop', account, exchange_feed)
        self._market = market
        self._side = side
        self._size = size
        self._attempts = attempts
        self._width = width
        self._kwargs = kwargs

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        account = self.get_account()
        exchange_feed = self.get_exchange_feed()

        for attempt in range(self._attempts):
            try:
                underlying_market = self._exchange_feed.get_ticker(self._market)
                mid_price = (underlying_market['ask'] + underlying_market['bid']) / 2
                trail_value = mid_price * self._width
                break

            except:
                # weird issue where the first time we subscribe to a websocket we can sometimes get
                # a {} response; so probably just retry...
                time.sleep(1)
                continue
        else:
            # we failed all attempts (didn't break from loop)
            raise Exception("Exchange feed issue")


        try:
            if self._side == 'sell':
                trail_value = -trail_value

            status = account.place_conditional_order(market = self._market, side = self._side, size = self._size, type = self._type,
                                trail_value = trail_value, **self._kwargs)
        
        except:
            status = OrderStatus(order_id = -1,
                            created_time = datetime.datetime.utcnow(),
                            market = self._market,
                            type = self._type,
                            side = self._side,
                            size = self._size,
                            filled_size = 0,
                            status = "closed"
            )

        self.set_id(status.order_id)
        return status


    def cancel(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot cancel non-executed order.")

        if self.failed():
            return

        account = self.get_account()
        return account.cancel_order(self.get_id(), conditional_order = True)


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


    def get_status(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot poll non-executed order.")
        
        if self.failed():
            status = OrderStatus(order_id = -1,
                            created_time = None,
                            market = self._market,
                            type = self._type,
                            side = self._side,
                            size = self._size,
                            filled_size = 0,
                            status = "closed"
            )

        else:
            account = self.get_account()
            status = account.get_conditional_order_status(self._market, self.get_id())

        return status