from typing import Optional
import time
import datetime

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.execution_handler.order_status import OrderStatus
from cryptomancer.account import Account
from cryptomancer.exchange_feed import ExchangeFeed

class TrailingStopOrder(Order):
    def __init__(self, account: Account, market: str, side: str, size: float, 
                    trail_value: int, **kwargs):
        super().__init__('trailing_stop', account, None)
        self._market = market
        self._side = side
        self._size = size
        self._trail_value = trail_value
        self._kwargs = kwargs

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        account = self.get_account()
        
        try:
            trail_value = self._trail_value if self._side == 'buy' else -self._trail_value

            status = account.place_conditional_order(market = self._market, 
                                                    side = self._side, 
                                                    size = self._size, 
                                                    type = self._type,
                                                    trail_value = trail_value, 
                                                    **self._kwargs)
        
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
        parameters = self._kwargs
        parameters['trail_value'] = self._trail_value

        return parameters

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
                            average_fill_price = None,
                            status = "closed",
                            parameters = self._get_parameters(),
                            exception = self._exception
            )

        else:
            account = self.get_account()
            status = account.get_conditional_order_status(self._market, self.get_id())

        return status