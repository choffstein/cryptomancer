from typing import Optional
import time
import datetime

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.account import Account
from cryptomancer.exchange_feed import ExchangeFeed


class AutoLimitOrderDollars(Order):
    def __init__(self, account: Account, exchange_feed: ExchangeFeed, 
                 market: str, side: str, size_usd: float, 
                 attempts: Optional[int] = 5, width: Optional[float] = 0.001,
                 **kwargs):
        super().__init__('limit_auto_dollars', account, exchange_feed)
        self._market = market
        self._side = side
        self._size_usd = size_usd
        self._size = None
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
                
                if self._side == 'buy':
                    target_underlying_px = underlying_market['ask'] * (1. + self._width)
                else:
                    target_underlying_px = underlying_market['bid'] * (1. - self._width)
                
                break

            except:
                # weird issue where the first time we subscribe to a websocket we can sometimes get
                # a {} response; so probably just retry...
                time.sleep(1)
                continue
        else:
            # we failed all attempts (didn't break from loop)
            raise Exception("Exchange feed issue")

        self._size = self._size_usd / target_underlying_px

        try:
            status = account.place_order(market = self._market, side = self._side, price = target_underlying_px, 
                                    size = self._size, type = "limit", **self._kwargs)
        
        except:
            status = OrderStatus(order_id = -1,
                            created_time = datetime.datetime.utcnow(),
                            market = self._market,
                            type = self._type,
                            side = self._side,
                            size = self._size,
                            filled_size = 0,
                            average_fill_price = None,
                            status = "closed"
            )

        self.set_id(status.order_id)
        return status

    @session_required
    def rollback(self):
        if not self.get_id() or self.failed():
            return

        try:
            self.cancel()
            self.wait_until_closed()
        except:
            pass            

        order_status = self.get_status()
        filled = order_status.size_filled

        if filled > 1e-8:
            side = "buy" if self._side == "sell" else "sell"
            account = self.get_account()
            status = account.place_order(market = self._market, side = side, price = None, 
                                    size = filled, type = "market", ioc = True)
            self.set_id(status.order_id)

            self.wait_until_closed()

