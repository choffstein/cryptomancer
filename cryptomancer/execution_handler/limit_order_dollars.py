from typing import Optional
import time
import datetime

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.account import Account


class LimitOrderDollars(Order):
    def __init__(self, account: Account, market: str, side: str,
                    size_usd: float, price: float, **kwargs):
        super().__init__('limit_dollars', account, None)
        self._market = market
        self._side = side
        self._size_usd = size_usd
        self._size = None
        self._price = price
        self._kwargs = kwargs

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        account = self.get_account()
        self._size = self._size_usd / self._price

        try:
            status = account.place_order(market = self._market, side = self._side, price = self._price, 
                                    size = self._size, type = "limit", **self._kwargs)
        
        except Exception as e:
            self._exception = str(e)

            status = OrderStatus(order_id = -1,
                            created_time = datetime.datetime.utcnow(),
                            market = self._market,
                            type = self._type,
                            side = self._side,
                            size = self._size,
                            filled_size = 0,
                            average_fill_price = None,
                            status = "closed",
                            parameters = self._get_parameters(),
                            exception = self._exception
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

