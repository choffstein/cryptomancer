from typing import Optional
import time

from cryptomancer.execution_handler import Order, session_required
from cryptomancer.account import Account
import cryptomancer.exchange_feed as exchange_feed


class MarketOrder(Order):
    def __init__(self, account: Account, market: str, side: str, size: float):
        super().__init__(account, None)
        self._market = market
        self._side = side
        self._size = size

    @session_required
    def submit(self) -> dict:
        if self.get_id():
            raise Exception("Cannot execute already working or finished market order.")

        account = self.get_account()
        status = account.place_order(market = self._market, side = self._side, price = None, 
                                    size = self._size, type = "market", ioc = True)
        self.set_id(status.order_id)
        return status

    @session_required
    def cancel(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot cancel non-executed order.")

        account = self.get_account()
        return account.cancel_order(self.get_id())

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
        filled = order_status.size_filled

        if filled > 1e-8:
            side = "buy" if self._side == "sell" else "sell"
            account = self.get_account()
            status = account.place_order(market = self._market, side = side, price = None, 
                                    size = filled, type = "market", ioc = True)
            self.set_id(status.order_id)

            self.wait_until_closed()

    @session_required 
    def get_status(self) -> dict:
        if not self.get_id():
            raise Exception("Cannot poll non-executed order.")
        
        account = self.get_account()
        return account.get_order_status(self.get_id())

    @session_required
    def is_pending(self) -> bool:
        return (self.get_id() is None)

    @session_required
    def is_submitted(self) -> bool:
        return not self.is_pending()

    @session_required
    def is_closed(self) -> bool:
        if not self.get_id():
            raise Exception("Cannot poll non-executed order.")
        order_status = self.get_status()
        return (order_status.status == 'closed')

    @session_required
    def wait_until_closed(self):
        while True:
            if self.is_closed():
                break
            # TODO: Better sleep method
            time.sleep(1)