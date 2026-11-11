from dotenv import load_dotenv
load_dotenv()
from bybit_api import BybitClient
c = BybitClient()
print("balance:", c.get_balance())
for p in c.get_positions():
    print(p["symbol"], p["side"], p["size"], "pnl", p["unrealisedPnl"])
