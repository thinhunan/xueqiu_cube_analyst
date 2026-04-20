# 配置项
import os


def _load_cookie() -> str:
    cookie_path = os.path.expanduser("~/agents_documents/xueqiu_cookies.txt")
    with open(cookie_path, "r", encoding="utf-8") as f:
        return f.read().strip()


COOKIE = _load_cookie()
CUBE_DATA_URL = "https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol=<cube_symbol>"
HISTORY_DATA_URL = "https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=<cube_symbol>&count=20&page=1"
CUBE_LINK_URL = "https://xueqiu.com/P/<cube_symbol>"
TRADE_COST = 0.00068
ANNUAL_RANK_URL = "https://xueqiu.com/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=annualized_gain_rate"
MONTHLY_RANK_URL = "https://xueqiu.com/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=monthly_gain"