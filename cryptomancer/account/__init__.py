from typing import Optional, List
from cryptomancer.account.position import Position

class Account(object):
    def __init__(self, account_name):
        self._account_name = account_name

    def account_name(self):
        return self._account_name

    def get_positions(self) -> List[Position]:
        raise NotImplementedError

    def get_open_orders(self, market: Optional[str] = None) -> List[dict]:
        raise NotImplementedError

    def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit', reduce_only: bool = False, ioc: bool = False, post_only: bool = False, client_id: Optional[str] = None) -> dict:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> dict:
        raise NotImplementedError

    def get_deposit_address(self, ticker: str, method: Optional[str] = None) -> dict:
        raise NotImplementedError

    def request_withdrawal(self, coin: str, size: float, address: str, password: Optional[str] = None, code: Optional[str] = None):
        raise NotImplementedError