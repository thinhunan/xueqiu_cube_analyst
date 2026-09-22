import time
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests

from config import (
    ANNUAL_RANK_URL,
    CUBE_DATA_URL,
    HISTORY_DATA_URL,
    MONTHLY_RANK_URL,
    REQUEST_INTERVAL,
    REQUEST_RETRY_TIMES,
    USER_AGENT,
    XUEQIU_WEB_ORIGIN,
    _load_cookie,
)

_session: Optional[requests.Session] = None
_session_cookie: Optional[str] = None
_last_request_at = 0.0


def _parse_cookie_str(cookie: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for part in cookie.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, value = part.split("=", 1)
        result[name.strip()] = value
    return result


def get_session() -> requests.Session:
    """共享 Session：Cookie 进 jar，不要放进请求头。"""
    global _session, _session_cookie
    cookie = _load_cookie()
    if _session is not None and cookie == _session_cookie:
        return _session

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    for name, value in _parse_cookie_str(cookie).items():
        session.cookies.set(name, value, domain=".xueqiu.com", path="/")
    _session = session
    _session_cookie = cookie
    print(f"✅ 成功加载Cookie，长度: {len(cookie)}字符")
    return session


def _referer_for(url: str) -> str:
    parsed = urlparse(url)
    symbol = (parse_qs(parsed.query).get("cube_symbol") or [""])[0]
    if symbol:
        return f"{XUEQIU_WEB_ORIGIN}/P/{symbol}"
    return f"{XUEQIU_WEB_ORIGIN}/"


def _ensure_json_body(resp: requests.Response) -> None:
    text = (resp.text or "").strip()
    if not text:
        raise requests.RequestException(
            f"空响应 status={resp.status_code} url={resp.url}"
        )
    ctype = (resp.headers.get("content-type") or "").lower()
    if "json" in ctype or text[:1] in "{[":
        return
    snippet = text[:120].replace("\n", " ")
    raise requests.RequestException(
        f"非JSON响应 status={resp.status_code} url={resp.url} body={snippet}"
    )


def _throttle() -> None:
    global _last_request_at
    elapsed = time.time() - _last_request_at
    wait = REQUEST_INTERVAL - elapsed
    if _last_request_at > 0 and wait > 0:
        time.sleep(wait)


def xueqiu_get(url: str, timeout: int = 30) -> requests.Response:
    """GET 雪球网页 API：限速、WAF/限流重试。"""
    global _last_request_at
    last_exc: Optional[Exception] = None
    total = REQUEST_RETRY_TIMES + 1
    session = get_session()

    for attempt in range(1, total + 1):
        _throttle()
        try:
            resp = session.get(
                url,
                headers={"Referer": _referer_for(url)},
                timeout=timeout,
            )
            _last_request_at = time.time()
            resp.raise_for_status()
            _ensure_json_body(resp)
            if attempt > 1:
                print(f"第 {attempt} 次重试成功: {url}")
            return resp
        except Exception as e:
            last_exc = e
            _last_request_at = time.time()
            status = (
                e.response.status_code
                if isinstance(e, requests.HTTPError) and e.response is not None
                else None
            )
            body = ""
            if isinstance(e, requests.HTTPError) and e.response is not None:
                body = (e.response.text or "")[:200]
            is_waf = "非JSON" in str(e) or "空响应" in str(e) or "<script>" in body
            is_rate = status == 400 and ("110017" in body or "过于频繁" in body)
            is_auth = status == 400 and "400016" in body
            if is_auth or attempt >= total:
                break
            sleep_seconds = 5 if (is_waf or is_rate) else min(attempt, 3)
            print(
                f"请求失败，第 {attempt}/{total} 次: {e}，{sleep_seconds} 秒后重试"
            )
            time.sleep(sleep_seconds)

    raise last_exc or RuntimeError(f"请求失败: {url}")


def load_cube_data(cube_symbol):
    """
    根据cube_symbol请求雪球组合数据

    Args:
        cube_symbol (str): 组合代码，如ZH3186221

    Returns:
        list: 返回的数据列表，如果请求失败返回None
    """
    try:
        url = CUBE_DATA_URL.replace("<cube_symbol>", cube_symbol)
        response = xueqiu_get(url)
        data = response.json()
        print(f"成功获取组合 {cube_symbol} 的数据")
        return data
    except requests.exceptions.RequestException as e:
        print(f"请求数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取数据时发生未知错误: {e}")
        return None


def load_rebalancing_history(cube_symbol):
    """
    根据cube_symbol请求雪球组合调仓历史数据

    Args:
        cube_symbol (str): 组合代码，如ZH3186221

    Returns:
        dict: 返回的调仓历史数据，如果请求失败返回None
    """
    try:
        url = HISTORY_DATA_URL.replace("<cube_symbol>", cube_symbol)
        response = xueqiu_get(url)
        data = response.json()
        print(f"成功获取组合 {cube_symbol} 的调仓历史数据")
        return data
    except requests.exceptions.RequestException as e:
        print(f"请求调仓历史数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析调仓历史JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取调仓历史数据时发生未知错误: {e}")
        return None


def load_annual_rank_data():
    """
    获取年收益榜单数据

    Returns:
        dict: 返回的年榜数据，如果请求失败返回None
    """
    try:
        response = xueqiu_get(ANNUAL_RANK_URL)
        data = response.json()
        print(f"成功获取年收益榜单数据，共 {data.get('count', 0)} 个组合")
        return data
    except requests.exceptions.RequestException as e:
        print(f"请求年榜数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析年榜JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取年榜数据时发生未知错误: {e}")
        return None


def load_monthly_rank_data():
    """
    获取月收益榜单数据

    Returns:
        dict: 返回的月榜数据，如果请求失败返回None
    """
    try:
        response = xueqiu_get(MONTHLY_RANK_URL)
        data = response.json()
        print(f"成功获取月收益榜单数据，共 {data.get('count', 0)} 个组合")
        return data
    except requests.exceptions.RequestException as e:
        print(f"请求月榜数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析月榜JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取月榜数据时发生未知错误: {e}")
        return None


if __name__ == "__main__":
    test_symbol = "ZH3186221"
    result = load_cube_data(test_symbol)
    if result:
        print(f"获取到 {len(result)} 条数据")
        if result and len(result) > 0:
            print(f"第一条数据示例: {result[0]}")
