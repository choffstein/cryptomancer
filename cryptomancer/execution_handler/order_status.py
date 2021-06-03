
from dataclasses import dataclass
import datetime

from typing import Optional

@dataclass
class OrderStatus:
     order_id: int
     created_time: datetime.datetime
     market: str
     type: str
     side: str
     size: float
     status: str
     filled_size: float
     average_fill_price: float
     parameters: Optional[dict] = None