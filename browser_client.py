#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""用 Playwright 真实浏览器上下文访问雪球，规避直连 API 的 WAF/风控。

用法:
  python browser_client.py --login          # 打开可见浏览器，手动登录后保存会话
  python browser_client.py --test ZH3186221  # 用已保存会话试拉一条净值
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import BrowserContext, Page, Playwright, sync_playwright

from config import REQUEST_RETRY_TIMES, USER_AGENT, XUEQIU_WEB_ORIGIN

PROFILE_DIR = Path.home() / "agents_documents" / "xueqiu_browser_profile"
COOKIES_FILE = Path.home() / "agents_documents" / "xueqiu_cookies.txt"
class BrowserHttpError(RuntimeError):
    def __init__(self, status_code: int, url: str, body: str):
        self.status_code = status_code
        self.url = url
        self.body = body
        snippet = (body or "")[:200].replace("\n", " ")
        super().__init__(f"HTTP {status_code} url={url} body={snippet}")


class BrowserResponse:
    """兼容 data_loader 里对 response.json() / raise_for_status() 的用法。"""

    def __init__(self, status_code: int, text: str, url: str):
        self.status_code = status_code
        self.text = text
        self.url = url
        self.headers: Dict[str, str] = {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise BrowserHttpError(self.status_code, self.url, self.text)

    def json(self) -> Any:
        return json.loads(self.text)


class XueqiuBrowser:
    def __init__(self) -> None:
        self._pw: Optional[Playwright] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._headless = True

    @property
    def started(self) -> bool:
        return self._context is not None

    def start(self, headless: bool = True) -> None:
        if self._context is not None:
            if headless == self._headless:
                return
            self.close()

        PROFILE_DIR.mkdir(parents=True, exist_ok=True)
        self._headless = headless
        self._pw = sync_playwright().start()
        self._context = self._pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=headless,
            viewport={"width": 1280, "height": 900},
            locale="zh-CN",
            user_agent=USER_AGENT,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._seed_cookies_from_file()
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        print(f"✅ Playwright 已启动（headless={headless}，profile={PROFILE_DIR}）")

    def close(self) -> None:
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
        if self._pw is not None:
            try:
                self._pw.stop()
            except Exception:
                pass
        self._context = None
        self._page = None
        self._pw = None

    def _seed_cookies_from_file(self) -> None:
        """首次或 profile 里无登录态时，把现有 cookie 文件注入浏览器。"""
        assert self._context is not None
        if self.is_logged_in():
            return
        if not COOKIES_FILE.exists():
            return
        raw = COOKIES_FILE.read_text(encoding="utf-8").strip()
        if not raw:
            return
        cookies: List[Dict[str, Any]] = []
        for part in raw.split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            name, value = part.split("=", 1)
            name, value = name.strip(), value.strip()
            if not name:
                continue
            cookies.append(
                {
                    "name": name,
                    "value": value,
                    "domain": ".xueqiu.com",
                    "path": "/",
                }
            )
        if cookies:
            self._context.add_cookies(cookies)
            print(f"✅ 已从 {COOKIES_FILE.name} 注入 {len(cookies)} 个 Cookie")

    def is_logged_in(self) -> bool:
        if self._context is None:
            return False
        cookies = {
            c["name"]: c.get("value", "")
            for c in self._context.cookies("https://www.xueqiu.com")
        }
        if cookies.get("xq_a_token") and cookies.get("u"):
            return True
        if cookies.get("xq_is_login") in ("1", "true", "True"):
            return bool(cookies.get("xq_a_token") or cookies.get("u"))
        return False

    def export_cookies_to_file(self) -> None:
        """把浏览器 Cookie 写回文本文件，方便其它工具复用。"""
        assert self._context is not None
        cookies = self._context.cookies("https://www.xueqiu.com")
        if not cookies:
            return
        line = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
        COOKIES_FILE.write_text(line + "\n", encoding="utf-8")
        print(f"✅ Cookie 已导出到 {COOKIES_FILE}")

    def interactive_login(self, timeout_seconds: int = 300) -> None:
        """打开可见浏览器，等你手动登录。"""
        del timeout_seconds  # 保留参数兼容；改为按 Enter 确认
        self.start(headless=False)
        assert self._page is not None
        print("=" * 60)
        print("请在弹出的浏览器中登录雪球账号")
        print("登录完成后回到本终端按 Enter")
        print("=" * 60)
        self._page.goto(f"{XUEQIU_WEB_ORIGIN}/", wait_until="domcontentloaded")
        input("完成后按 Enter 继续… ")
        if not self.is_logged_in():
            raise RuntimeError("仍未检测到登录 Cookie（需要 xq_a_token / u）")
        print("✅ 检测到已登录")
        self.export_cookies_to_file()
        # 登录完成后切回 headless 供后续批量请求
        self.close()
        self.start(headless=True)

    def ensure_ready(self, allow_interactive_login: bool = True) -> None:
        if not self.started:
            self.start(headless=True)
        if self.is_logged_in():
            self._warm_origin()
            return
        if allow_interactive_login:
            print("⚠️ 未检测到雪球登录态，将打开浏览器供你登录…")
            self.interactive_login()
            self._warm_origin()
            return
        raise RuntimeError(
            "未登录雪球。请先运行: python browser_client.py --login"
        )

    def _warm_origin(self) -> None:
        """先打开雪球首页，让后续 fetch 带上真实页面上下文。"""
        assert self._page is not None
        url = self._page.url or ""
        if "xueqiu.com" in url:
            return
        self._page.goto(f"{XUEQIU_WEB_ORIGIN}/", wait_until="domcontentloaded", timeout=60000)

    def get(self, url: str, timeout_ms: int = 30000) -> BrowserResponse:
        """在页面内用 fetch 发 GET，带 credentials，更接近真实浏览行为。"""
        self.ensure_ready(allow_interactive_login=True)
        assert self._page is not None
        self._warm_origin()

        last_exc: Optional[Exception] = None
        total = REQUEST_RETRY_TIMES + 1
        for attempt in range(1, total + 1):
            try:
                result = self._page.evaluate(
                    """async ({url, timeoutMs}) => {
                        const controller = new AbortController();
                        const timer = setTimeout(() => controller.abort(), timeoutMs);
                        try {
                            const resp = await fetch(url, {
                                method: 'GET',
                                credentials: 'include',
                                headers: {
                                    'Accept': 'application/json, text/plain, */*',
                                },
                                signal: controller.signal,
                            });
                            const text = await resp.text();
                            return {
                                status: resp.status,
                                ok: resp.ok,
                                text,
                                finalUrl: resp.url,
                            };
                        } finally {
                            clearTimeout(timer);
                        }
                    }""",
                    {"url": url, "timeoutMs": timeout_ms},
                )
                resp = BrowserResponse(
                    status_code=int(result["status"]),
                    text=result.get("text") or "",
                    url=result.get("finalUrl") or url,
                )
                text = (resp.text or "").strip()
                if not text:
                    raise RuntimeError(f"空响应 status={resp.status_code} url={resp.url}")
                if not (text[:1] in "{[" or "json" in text[:40].lower()):
                    # 可能是 WAF HTML
                    if "<script>" in text or "<html" in text.lower():
                        raise RuntimeError(
                            f"非JSON/疑似WAF status={resp.status_code} "
                            f"body={text[:120].replace(chr(10), ' ')}"
                        )
                if attempt > 1:
                    print(f"第 {attempt} 次重试成功: {url}")
                return resp
            except Exception as e:
                last_exc = e
                if attempt >= total:
                    break
                sleep_seconds = min(2 * attempt, 6)
                print(f"浏览器请求失败，第 {attempt}/{total} 次: {e}，{sleep_seconds}s 后重试")
                time.sleep(sleep_seconds)

        raise last_exc or RuntimeError(f"请求失败: {url}")


_browser: Optional[XueqiuBrowser] = None


def get_browser() -> XueqiuBrowser:
    global _browser
    if _browser is None:
        _browser = XueqiuBrowser()
    return _browser


def close_browser() -> None:
    global _browser
    if _browser is not None:
        _browser.close()
        _browser = None


def main() -> None:
    parser = argparse.ArgumentParser(description="雪球 Playwright 浏览器客户端")
    parser.add_argument("--login", action="store_true", help="打开浏览器手动登录并保存会话")
    parser.add_argument("--test", metavar="CUBE", help="试拉组合净值 JSON，如 ZH3186221")
    parser.add_argument("--headed", action="store_true", help="测试时用可见浏览器")
    args = parser.parse_args()

    browser = get_browser()
    try:
        if args.login:
            browser.interactive_login()
            print("登录会话已保存，后续脚本可直接 headless 使用")
            return

        browser.start(headless=not args.headed)
        browser.ensure_ready(allow_interactive_login=True)

        symbol = args.test or "ZH3186221"
        url = (
            f"{XUEQIU_WEB_ORIGIN}/cubes/nav_daily/all.json"
            f"?cube_symbol={symbol}"
        )
        print(f"试请求: {url}")
        resp = browser.get(url)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            print(f"✅ 成功，共 {len(data)} 条净值")
            if data:
                print(f"示例: {data[0]}")
        else:
            print(f"✅ 成功，响应类型={type(data).__name__} keys={list(data)[:8]}")
    finally:
        close_browser()


if __name__ == "__main__":
    main()
