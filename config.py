# 配置项
import os


def _load_cookie() -> str:
    cookie_path = os.path.expanduser("~/agents_documents/xueqiu_cookies.txt")
    with open(cookie_path, "r", encoding="utf-8") as f:
        return f.read().strip()


COOKIE = _load_cookie()

# 网页 API 必须直连 www。xueqiu.com 会 302 到 www，requests 跨 Host 会丢掉 Cookie 头。
XUEQIU_WEB_ORIGIN = "https://www.xueqiu.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
REQUEST_INTERVAL = 2.0
REQUEST_RETRY_TIMES = 3

CUBE_DATA_URL = XUEQIU_WEB_ORIGIN + "/cubes/nav_daily/all.json?cube_symbol=<cube_symbol>"
HISTORY_DATA_URL = (
    XUEQIU_WEB_ORIGIN
    + "/cubes/rebalancing/history.json?cube_symbol=<cube_symbol>&count=50&page=1"
)
CUBE_LINK_URL = "https://xueqiu.com/P/<cube_symbol>"
TRADE_COST = 0.00068
ANNUAL_RANK_URL = (
    XUEQIU_WEB_ORIGIN
    + "/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=annualized_gain_rate"
)
MONTHLY_RANK_URL = (
    XUEQIU_WEB_ORIGIN
    + "/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=monthly_gain"
)
