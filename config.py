# 配置项
COOKIE = "###填入你自己的cookie###"
CUBE_DATA_URL = "https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol=<cube_symbol>"
HISTORY_DATA_URL = "https://xueqiu.com/cubes/rebalancing/history.json?cube_symbol=<cube_symbol>&count=20&page=1"
CUBE_LINK_URL = "https://xueqiu.com/P/<cube_symbol>"
TRADE_COST = 0.00068 # 交易成本，默认为0.00068，如果需要修改，请修改此值
ANNUAL_RANK_URL = "https://xueqiu.com/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=annualized_gain_rate"
MONTHLY_RANK_URL = "https://xueqiu.com/cubes/discover/rank/cube/list.json?category=12&count=10&market=cn&profit=monthly_gain"