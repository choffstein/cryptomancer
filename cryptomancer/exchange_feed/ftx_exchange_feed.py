from typing import List, Dict, Optional, Tuple
import numpy

from cryptomancer.exchange_feed import ExchangeFeed
from cryptomancer.exchange_feed.ftx_wsocket_client import FtxWebsocketClient

class FtxExchangeFeed(ExchangeFeed):
    def __init__(self, account_name: Optional[str] = None, feed_endpoint: Optional[str] = None):
        self.wsocket_client = FtxWebsocketClient(account_name, feed_endpoint)

    def get_orders(self) -> Dict[int, Dict]:
        return self.wsocket_client.get_orders()

    def get_fills(self) -> List[Dict]:
        return self.wsocket_client.get_fills()

    def get_trades(self, market: str) -> List[Dict]:
        return self.wsocket_client.get_trades(market)

    def get_ticker(self, market: str) -> Dict:
        return self.wsocket_client.get_ticker(market)

    def get_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        return self.wsocket_client.get_orderbook(market)
        
    def get_cumulative_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        ob = self.get_orderbook(market)

        bids = ob['bids']
        asks = ob['asks']

        bids = sorted(bids, key = lambda bid: bid[0])
        bids = bids[::-1] # largest to smallest for the cumsum to work
        bid_x = [bid[0] for bid in bids]
        bid_y = [bid[1] for bid in bids]
        bid_y = numpy.cumsum(bid_y)

        ob['bids'] = list(zip(bid_x, bid_y))

        asks = sorted(asks, key = lambda ask: ask[0])
        ask_x = [ask[0] for ask in asks]
        ask_y = [ask[1] for ask in asks]
        ask_y = numpy.cumsum(ask_y)

        ob['asks'] = list(zip(ask_x, ask_y))

        return ob