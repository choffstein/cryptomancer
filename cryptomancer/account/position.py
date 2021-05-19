from dataclasses import dataclass

@dataclass
class Position:
     name: str
     kind: str
     size: float
     net_size: float
     side: str
     usd_value: float
