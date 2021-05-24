from typing import List, Dict, Optional, Tuple
import numpy

from cryptomancer.exchange_feed import ExchangeFeed
from cryptomancer.exchange_feed.ftx_wsocket_client import FtxWebsocketClient

class FtxExchangeFeed(ExchangeFeed):
    def __init__(self, account_name: Optional[str] = None):
        self.wsocket_client = FtxWebsocketClient(account_name)
        
    def orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        ob = self.wsocket_client.get_orderbook(market)
        return ob

    def cumulative_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        ob = self.orderbook(market)

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

    def get_current_market(self, market: str) -> Dict:
        m = self.wsocket_client.get_ticker(market)
        return m