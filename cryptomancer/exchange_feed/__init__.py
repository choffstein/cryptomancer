from typing import List, Dict, Optional, Tuple

class ExchangeFeed(object):
    def __init__(self):
        pass

    def get_orders(self) -> Dict[int, Dict]:
        raise NotImplementedError

    def get_fills(self) -> List[Dict]:
        raise NotImplementedError

    def get_bid_offer(self, market: str) -> List[Dict]:
        raise NotImplementedError

    def get_ticker(self, market: str) -> Dict:
        raise NotImplementedError

    def get_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        raise NotImplementedError
        
    def get_cumulative_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        raise NotImplementedError