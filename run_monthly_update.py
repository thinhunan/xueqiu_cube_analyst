#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""launchctl 调度入口：跑 update_choosen.main()，把结果通过飞书机器人发出来。"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import datetime
from pathlib import Path

import requests

# 让脚本能 import 同目录的模块（launchd 不一定切到工作目录）
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from update_choosen import main as run_update_choosen

FEISHU_WEBHOOK = (
    "https://open.feishu.cn/open-apis/bot/v2/hook/da11b2b9-5b13-4ac9-a416-51a2f50ccb70"
)


def _feishu(title: str, body: str) -> None:
    try:
        text = f"{title}\n\n{body}"
        requests.post(
            FEISHU_WEBHOOK,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=10,
        )
    except Exception as e:
        logging.error(f"飞书推送失败: {e}")


def _format_cubes(items) -> str:
    if not items:
        return "（无）"
    lines = []
    for c in items:
        line = f"  • {c.get('name', '')}({c.get('cube') or c.get('code') or ''})"
        if "score" in c:
            line += f" score={c['score']:.2f}"
        if "percent" in c:
            line += f" percent={c['percent']}"
        if "old_percent" in c:
            line += f" old_percent={c['old_percent']}"
        lines.append(line)
    return "\n".join(lines)


def _format_result(result: dict) -> str:
    parts = [
        f"状态: {result.get('status')}",
        f"信息: {result.get('message')}",
        f"运行时间: {result.get('run_at')}",
        "",
        "—— 新选中 ——",
        _format_cubes(result.get("new_choosen", [])),
        "",
        "—— 新增建仓 ——",
        _format_cubes(result.get("added_cubes", [])),
        "",
        "—— 移除/清仓 ——",
        _format_cubes(result.get("removed_cubes", [])),
        "",
        f"指令文件: {result.get('command_file') or '（无）'}",
        f"指令数: {result.get('operations_count', 0)}",
    ]
    errs = result.get("sync_errors") or []
    if errs:
        parts.append("")
        parts.append("⚠️ 同步错误：")
        for e in errs:
            parts.append(f"  - {e}")
    return "\n".join(parts)


def main() -> int:
    start = datetime.now()
    try:
        result = run_update_choosen()
    except Exception as e:
        tb = traceback.format_exc()
        logging.exception(f"update_choosen 执行异常: {e}")
        _feishu(
            "❌ 月度组合更新失败",
            f"开始: {start:%Y-%m-%d %H:%M:%S}\n异常: {e}\n\n{tb[:1500]}",
        )
        return 1

    if not isinstance(result, dict):
        _feishu(
            "⚠️ 月度组合更新返回异常",
            f"start: {start:%Y-%m-%d %H:%M:%S}\nresult={result!r}",
        )
        return 2

    status = result.get("status", "unknown")
    icon = {"ok": "✅", "empty": "ℹ️", "error": "❌"}.get(status, "⚠️")
    title = f"{icon} 月度雪球组合更新 - {status}"
    _feishu(title, _format_result(result))

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if status == "ok" else 0  # 即便 empty 也别让 launchd 标记失败


if __name__ == "__main__":
    sys.exit(main())
