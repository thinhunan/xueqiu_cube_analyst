import math
import re
import time
from typing import Dict, Optional
from urllib.parse import parse_qs, urlparse

import requests
from pathlib import Path

from config import (
    ANNUAL_RANK_URL,
    CUBE_DATA_URL,
    HISTORY_DATA_URL,
    MONTHLY_RANK_URL,
    REQUEST_INTERVAL,
    REQUEST_RETRY_TIMES,
    USER_AGENT,
    XUEQIU_WEB_ORIGIN,
)
from history_store import (
    build_history_result,
    get_existing_ids,
    get_record_count,
    has_records,
    save_records,
)

HISTORY_PAGE_SIZE = 50
HISTORY_PAGE_DELAY = 0.35
HISTORY_PAGE_MAX_RETRIES = 5

# 使用Path.home()确保正确解析家目录
XUEQIU_COOKIES_FILE = Path.home() / "agents_documents" / "xueqiu_cookies.txt"

# 缓存Cookie，避免重复读取
_COOKIE_CACHE = None
_session: Optional[requests.Session] = None
_session_cookie: Optional[str] = None
_last_request_at = 0.0


def _read_xueqiu_cookie() -> str:
    """
    读取雪球Cookie文件
    
    修复内容:
    1. 使用Path.home()确保路径正确解析
    2. 延迟加载（函数调用时读取，非模块导入时）
    3. 添加缓存避免重复读取
    4. 更好的错误信息
    """
    global _COOKIE_CACHE
    
    # 如果有缓存，直接返回
    if _COOKIE_CACHE is not None:
        return _COOKIE_CACHE
    
    # 检查文件是否存在
    if not XUEQIU_COOKIES_FILE.exists():
        # 列出可能的路径信息用于调试
        home_dir = Path.home()
        openclaw_dir = home_dir / ".openclaw"
        documents_dir = openclaw_dir / "Documents"
        
        error_msg = (
            f"未找到雪球Cookie文件: {XUEQIU_COOKIES_FILE}\n"
            f"调试信息:\n"
            f"  - 家目录: {home_dir} (存在: {home_dir.exists()})\n"
            f"  - .openclaw目录: {openclaw_dir} (存在: {openclaw_dir.exists()})\n"
            f"  - Documents目录: {documents_dir} (存在: {documents_dir.exists()})\n"
            f"  - Cookie文件: {XUEQIU_COOKIES_FILE} (存在: {XUEQIU_COOKIES_FILE.exists()})"
        )
        raise FileNotFoundError(error_msg)
    
    try:
        cookie = XUEQIU_COOKIES_FILE.read_text(encoding="utf-8").strip()
    except Exception as e:
        raise IOError(f"读取Cookie文件失败: {e}") from e
    
    if not cookie:
        raise ValueError(f"Cookie文件为空: {XUEQIU_COOKIES_FILE}")
    
    # 缓存结果
    _COOKIE_CACHE = cookie
    print(f"✅ 成功加载Cookie，长度: {len(cookie)}字符")
    return cookie


def get_cookie() -> str:
    """获取Cookie（带缓存）"""
    return _read_xueqiu_cookie()


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
    cookie = get_cookie()
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
        
    except FileNotFoundError as e:
        print(f"❌ Cookie文件错误: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"请求数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取数据时发生未知错误: {e}")
        return None


def _build_history_url(cube_symbol, page, count=HISTORY_PAGE_SIZE):
    url = HISTORY_DATA_URL.replace("<cube_symbol>", cube_symbol)
    url = re.sub(r'count=\d+', f'count={count}', url)
    url = re.sub(r'page=\d+', f'page={page}', url)
    return url


def _fetch_history_page(cube_symbol, page, max_retries=HISTORY_PAGE_MAX_RETRIES):
    url = _build_history_url(cube_symbol, page)
    for attempt in range(max_retries):
        try:
            response = xueqiu_get(url)
            data = response.json()
            if data.get('error_code'):
                if attempt < max_retries - 1:
                    time.sleep(HISTORY_PAGE_DELAY * (attempt + 2))
                continue
            return data
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(HISTORY_PAGE_DELAY * (attempt + 2))
    return None


def _calc_history_total_pages(total_count):
    if total_count <= 0:
        return 1
    return math.ceil(total_count / HISTORY_PAGE_SIZE)


def _fetch_all_history_pages(cube_symbol):
    """无本地缓存时，分页拉取全部调仓历史。"""
    first_page = _fetch_history_page(cube_symbol, 1)
    if not first_page:
        return None, []

    total_count = first_page.get('totalCount', 0) or 0
    all_records = list(first_page.get('list', []) or [])
    if not all_records and total_count == 0:
        return None, []

    total_pages = _calc_history_total_pages(total_count)
    failed_pages = []
    for page in range(2, total_pages + 1):
        time.sleep(HISTORY_PAGE_DELAY)
        data = _fetch_history_page(cube_symbol, page)
        if not data:
            failed_pages.append(page)
            continue
        batch = data.get('list', []) or []
        if not batch:
            failed_pages.append(page)
            continue
        all_records.extend(batch)

    for page in failed_pages:
        time.sleep(HISTORY_PAGE_DELAY * 2)
        data = _fetch_history_page(cube_symbol, page, max_retries=3)
        if data:
            batch = data.get('list', []) or []
            if batch:
                all_records.extend(batch)

    return total_count, all_records


def _fetch_incremental_history_pages(cube_symbol, existing_ids):
    """
    已有本地缓存时，从第 1 页（最新）往后拉，遇到已存在 record_id 即停止。
    """
    first_page = _fetch_history_page(cube_symbol, 1)
    if not first_page:
        return None, []

    total_count = first_page.get('totalCount', 0) or 0
    total_pages = _calc_history_total_pages(total_count)
    new_records = []
    hit_overlap = False

    for page in range(1, total_pages + 1):
        if page == 1:
            batch = list(first_page.get('list', []) or [])
        else:
            time.sleep(HISTORY_PAGE_DELAY)
            data = _fetch_history_page(cube_symbol, page)
            if not data:
                break
            batch = data.get('list', []) or []
        if not batch:
            break

        for rec in batch:
            record_id = rec.get('id')
            if record_id is not None and int(record_id) in existing_ids:
                hit_overlap = True
                break
            new_records.append(rec)

        if hit_overlap:
            break

    return total_count, new_records


def load_rebalancing_history(cube_symbol):
    """
    调仓历史：优先读 SQLite；无缓存则全量拉取，有缓存则增量拉取至与本地重复后停止。
    """
    try:
        cached = has_records(cube_symbol)

        if not cached:
            total_count, fetched = _fetch_all_history_pages(cube_symbol)
            if not fetched and not total_count:
                return None
            inserted = save_records(cube_symbol, fetched, total_count or len(fetched))
            result = build_history_result(cube_symbol, total_count)
            total_pages = _calc_history_total_pages(result['totalCount'])
            print(
                f"组合 {cube_symbol} 调仓历史全量拉取完成，"
                f"共 {total_pages} 页，入库 {inserted} 条，"
                f"本地 {result['fetchedCount']}/{result['totalCount']} 条"
            )
            return result

        existing_ids = get_existing_ids(cube_symbol)
        total_count, new_records = _fetch_incremental_history_pages(
            cube_symbol, existing_ids
        )
        if total_count is None:
            result = build_history_result(cube_symbol)
            if result.get('list'):
                print(
                    f"组合 {cube_symbol} 增量更新失败，使用本地缓存 "
                    f"{result['fetchedCount']} 条"
                )
                return result
            return None

        inserted = save_records(cube_symbol, new_records, total_count)
        result = build_history_result(cube_symbol, total_count)
        db_count = get_record_count(cube_symbol)
        if db_count < total_count:
            print(
                f"组合 {cube_symbol} 增量更新 +{inserted} 条，"
                f"本地 {db_count}/{total_count} 条（历史可能未全量拉取，可删库后重拉）"
            )
        else:
            print(
                f"组合 {cube_symbol} 增量更新 +{inserted} 条，"
                f"本地 {db_count}/{total_count} 条"
            )
        return result

    except FileNotFoundError as e:
        print(f"❌ Cookie文件错误: {e}")
        return None
    except Exception as e:
        print(f"获取调仓历史数据时发生未知错误: {e}")
        return None


def load_rebalancing_history_with_retry(cube_symbol, max_attempts=3, retry_delay=2.0):
    """带重试的调仓历史拉取"""
    last_result = None
    for attempt in range(max_attempts):
        last_result = load_rebalancing_history(cube_symbol)
        if last_result and last_result.get('list'):
            return last_result
        if attempt < max_attempts - 1:
            time.sleep(retry_delay * (attempt + 1))
    return last_result


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
        
    except FileNotFoundError as e:
        print(f"❌ Cookie文件错误: {e}")
        return None
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
        
    except FileNotFoundError as e:
        print(f"❌ Cookie文件错误: {e}")
        return None
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
    # 测试功能
    print(f"Cookie文件路径: {XUEQIU_COOKIES_FILE}")
    print(f"Cookie文件存在: {XUEQIU_COOKIES_FILE.exists()}")
    
    try:
        cookie = get_cookie()
        print(f"✅ Cookie加载成功，长度: {len(cookie)}字符")
        
        test_symbol = "ZH3186221"
        result = load_cube_data(test_symbol)
        if result:
            print(f"获取到 {len(result)} 条数据")
            if result and len(result) > 0:
                print(f"第一条数据示例: {result[0]}")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
