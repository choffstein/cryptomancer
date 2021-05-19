import ftx
from typing import Optional, List

import pandas

from cryptomancer.account import Account
from cryptomancer.execution_handler.order_status import OrderStatus

from cryptomancer.account.position import Position

import cryptomancer.local_secrets as local_secrets

class FtxAccount(Account):
    """
        This is a generic FTX Account account object that wraps around the FTX client.
    """
    def __init__(self, account_name: str):
        super().__init__(account_name)
        account_details = local_secrets.load(self._account_name)

        self.account = ftx.FtxClient(api_key = account_details["API_KEY"], 
                                    api_secret = account_details["API_SECRET"], 
                                    subaccount_name = account_details["SUBACCOUNT"])


    def get_positions(self) -> List[Position]:
        positions = []

        for coin in self.account.get_balances():
            if abs(coin['total']) > 1e-8:
                p = Position(name = coin['coin'],
                            kind = 'coin',
                            size = coin['total'],
                            net_size = coin['total'],
                            side = 'buy',
                            usd_value = coin['usdValue'])
                positions.append(p)

        for future in self.account.get_positions():
            if abs(future['netSize']) > 1e-8:
                p = Position(name = future['future'],
                        kind = 'perpetual' if 'PERP' in future['future'] else 'future',
                        size = future['size'],
                        net_size = future['netSize'],
                        side = future['side'],
                        usd_value = future['recentPnl'])

                positions.append(p)

        return positions

    def get_open_orders(self, market: Optional[str] = None) -> List[dict]:
        return self.account.get_open_orders(market = market)

    def place_order(self, market: str, side: str, price: float, size: float, type: str = 'limit', reduce_only: bool = False, ioc: bool = False, post_only: bool = False, client_id: Optional[str] = None) -> dict:
        order_status = self.account.place_order(market = market, side = side, price = price, size = size, type = type, 
                                 reduce_only = reduce_only, ioc = ioc, post_only = post_only, client_id = client_id)
        return OrderStatus(order_id = order_status['id'],
                            created_time = pandas.Timestamp(order_status['createdAt']).to_pydatetime(),
                            market = order_status['market'],
                            side = order_status['side'],
                            size = order_status['size'],
                            filled_size = order_status['filledSize'],
                            status = order_status['status'])


    def cancel_order(self, order_id: str) -> dict:
        return self.account.cancel_order(order_id = order_id)

    def get_order_status(self, order_id: str) -> dict:
        order_status = self.account.get_order_status(existing_order_id = order_id)
        return OrderStatus(order_id = order_status['id'],
                            created_time = pandas.Timestamp(order_status['createdAt']).to_pydatetime(),
                            market = order_status['market'],
                            side = order_status['side'],
                            size = order_status['size'],
                            filled_size = order_status['filledSize'],
                            status = order_status['status'])

    def get_deposit_address(self, ticker: str, method: Optional[str] = None) -> dict:
        return self.account.get_deposit_address(ticker = ticker, method = method)

    def request_withdrawal(self, coin: str, size: float, address: str, password: Optional[str] = None, code: Optional[str] = None):
        return self.account.request_withdrawal(coin = coin, size = size, address = address, password = password, code = code)