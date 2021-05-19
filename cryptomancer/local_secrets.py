import json
from typing import Dict

def load(fname: str) -> Dict:
    with open(".secrets/" + fname + ".json") as f:
        return json.load(f)