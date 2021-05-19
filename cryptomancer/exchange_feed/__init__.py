from typing import List, Dict, Optional, Tuple

class ExchangeFeed(object):
    def __init__(self):
        pass

    def orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        raise NotImplementedError

    def get_current_market(self, market: str) -> Dict:
        raise NotImplementedError