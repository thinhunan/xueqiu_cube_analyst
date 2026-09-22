import atexit
import math
import re
import time
from pathlib import Path

from browser_client import BrowserHttpError, BrowserResponse, close_browser, get_browser
from config import (
    ANNUAL_RANK_URL,
    CUBE_DATA_URL,
    HISTORY_DATA_URL,
    MONTHLY_RANK_URL,
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

atexit.register(close_browser)


def get_cookie() -> str:
    """读取 Cookie 文件（兼容旧逻辑 / 注入浏览器）。"""
    if not XUEQIU_COOKIES_FILE.exists():
        raise FileNotFoundError(f"未找到雪球Cookie文件: {XUEQIU_COOKIES_FILE}")
    cookie = XUEQIU_COOKIES_FILE.read_text(encoding="utf-8").strip()
    if not cookie:
        raise ValueError(f"Cookie文件为空: {XUEQIU_COOKIES_FILE}")
    return cookie


def _ensure_json_body(resp: BrowserResponse) -> None:
    text = (resp.text or "").strip()
    if not text:
        raise RuntimeError(f"空响应 status={resp.status_code} url={resp.url}")
    if text[:1] in "{[":
        return
    snippet = text[:120].replace("\n", " ")
    raise RuntimeError(
        f"非JSON响应 status={resp.status_code} url={resp.url} body={snippet}"
    )


def xueqiu_get(url: str, timeout: int = 30) -> BrowserResponse:
    """通过 Playwright 浏览器内 fetch 访问雪球 API。"""
    browser = get_browser()
    resp = browser.get(url, timeout_ms=timeout * 1000)
    resp.raise_for_status()
    _ensure_json_body(resp)
    return resp


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
    except (BrowserHttpError, RuntimeError) as e:
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
    url = re.sub(r"count=\d+", f"count={count}", url)
    url = re.sub(r"page=\d+", f"page={page}", url)
    return url


def _fetch_history_page(cube_symbol, page, max_retries=HISTORY_PAGE_MAX_RETRIES):
    url = _build_history_url(cube_symbol, page)
    for attempt in range(max_retries):
        try:
            response = xueqiu_get(url)
            data = response.json()
            if data.get("error_code"):
                if attempt < max_retries - 1:
                    time.sleep(HISTORY_PAGE_DELAY * (attempt + 2))
                continue
            return data
        except (BrowserHttpError, RuntimeError):
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

    total_count = first_page.get("totalCount", 0) or 0
    all_records = list(first_page.get("list", []) or [])
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
        batch = data.get("list", []) or []
        if not batch:
            failed_pages.append(page)
            continue
        all_records.extend(batch)

    for page in failed_pages:
        time.sleep(HISTORY_PAGE_DELAY * 2)
        data = _fetch_history_page(cube_symbol, page, max_retries=3)
        if data:
            batch = data.get("list", []) or []
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

    total_count = first_page.get("totalCount", 0) or 0
    total_pages = _calc_history_total_pages(total_count)
    new_records = []
    hit_overlap = False

    for page in range(1, total_pages + 1):
        if page == 1:
            batch = list(first_page.get("list", []) or [])
        else:
            time.sleep(HISTORY_PAGE_DELAY)
            data = _fetch_history_page(cube_symbol, page)
            if not data:
                break
            batch = data.get("list", []) or []
        if not batch:
            break

        for rec in batch:
            record_id = rec.get("id")
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
            total_pages = _calc_history_total_pages(result["totalCount"])
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
            if result.get("list"):
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
        if last_result and last_result.get("list"):
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
    except (BrowserHttpError, RuntimeError) as e:
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
    except (BrowserHttpError, RuntimeError) as e:
        print(f"请求月榜数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析月榜JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取月榜数据时发生未知错误: {e}")
        return None


if __name__ == "__main__":
    print(f"Cookie文件路径: {XUEQIU_COOKIES_FILE}")
    print(f"Cookie文件存在: {XUEQIU_COOKIES_FILE.exists()}")

    try:
        test_symbol = "ZH3186221"
        result = load_cube_data(test_symbol)
        if result:
            print(f"获取到 {len(result)} 条数据")
            if result and len(result) > 0:
                print(f"第一条数据示例: {result[0]}")
    except Exception as e:
        print(f"❌ 测试失败: {e}")
    finally:
        close_browser()
