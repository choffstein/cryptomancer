from typing import List, Dict, Optional, Tuple

from cryptomancer.exchange_feed import ExchangeFeed
from cryptomancer.exchange_feed.ftx_wsocket_client import FtxWebsocketClient

class FtxExchangeFeed(ExchangeFeed):
    def __init__(self, account_name: Optional[str] = None):
        self.wsocket_client = FtxWebsocketClient(account_name)
        
    def orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        ob = self.wsocket_client.get_orderbook(market)
        return ob

    def get_current_market(self, market: str) -> Dict:
        m = self.wsocket_client.get_ticker(market)
        return m