from typing import Optional
import time
import datetime

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.account import Account


class MarketOrder(Order):
    def __init__(self, account: Account, market: str, side: str, size: float, **kwargs):
        super().__init__('market', account, None)
        self._market = market
        self._side = side
        self._size = size
        self._kwargs = kwargs

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        account = self.get_account()
        try:
            status = account.place_order(market = self._market, side = self._side, price = None, 
                                    size = self._size, type = "market", **self._kwargs)
        
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

    def _get_parameters(self) -> dict:
        return self._kwargs

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
