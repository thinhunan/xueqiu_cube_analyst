#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""根据最新 choosen.csv 同步雪球组合到 xueqiu_follower/config.py，
并为新增/移除/降权（percent→0）的组合生成 position_sync 指令文件。

设计要点：
* 新选中前 5 名：percent=0.2、price_type=market_price
* 第 6 名：percent=0.0（仅观察）
* 已移除或被降为 0 的组合：按 old_percent 生成清仓 SELL 指令
* 新加入的组合：按 new_percent 生成建仓 BUY 指令
* fixed=True：仍保留在 follower 跟踪列表，调仓消息由 xueqiu_follower 照常同步；
  月度选股时仅「不移出、不修改 percent/price_type」，不参与 choosen 的权重重分配
* 数量计算 = total_assets × cube_percent × stock_weight / price，
  并按 A 股最小交易单位（100 股；688 科创板≥200）取整
"""

from __future__ import annotations

import ast
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parent.parent
_POSITION_SYNC_DIR = _REPO_ROOT / "position_sync"

# 加到 sys.path 末尾，避免遮蔽本目录的 config.py
if str(_POSITION_SYNC_DIR) not in sys.path:
    sys.path.append(str(_POSITION_SYNC_DIR))

from data_loader import load_rebalancing_history  # noqa: E402
from command_writer import write_command_file  # noqa: E402
from qmt_client import QmtClient  # noqa: E402

FOLLOWER_CONFIG_PATH = _REPO_ROOT / "xueqiu_follower" / "config.py"

TOP_N_TRADE = 5
TRADE_PERCENT = 0.2
WATCH_PERCENT = 0.0
DEFAULT_PRICE_TYPE = "market_price"


# ---------- 持仓快照（来自 cube 最新调仓后的 target_weight 即当前仓位） ----------

def _fetch_cube_current_holdings(cube_symbol: str) -> List[Dict[str, Any]]:
    """获取组合当前持仓（最近一次成功调仓的 target_weight）。

    返回: [{stock_code, stock_name, weight(0~1), price}]
    """
    data = load_rebalancing_history(cube_symbol)
    if not data or not isinstance(data, dict):
        return []
    items = data.get("list") or []
    for entry in items:
        if entry.get("status") != "success":
            continue
        holdings = entry.get("rebalancing_histories") or []
        out = []
        for h in holdings:
            tw = float(h.get("target_weight") or 0) / 100.0
            if tw <= 0:
                continue
            code = (h.get("stock_symbol") or "").upper()
            # 去掉 SH/SZ 前缀，position_sync 只要 6 位数字
            digits = re.sub(r"\D", "", code)
            if len(digits) < 6:
                continue
            out.append({
                "stock_code": digits[-6:],
                "stock_name": h.get("stock_name") or digits[-6:],
                "weight": tw,
                "price": float(h.get("price") or 0),
            })
        return out
    return []


# ---------- A 股最小交易单位调整 ----------

def _round_lot(code: str, qty: int, action: str, current_holding: int = 0) -> int:
    if qty <= 0:
        return 0
    if code.startswith("688"):
        if action == "sell":
            remaining = current_holding - qty
            if 0 < remaining < 200:
                return current_holding
        return qty if qty >= 200 else 0
    return (qty // 100) * 100


# ---------- 操作生成 ----------

def _ops_for_cube(cube_symbol: str,
                  cube_name: str,
                  action: str,
                  cube_percent: float,
                  total_assets: float,
                  current_account_positions: Dict[str, int]) -> List[Dict[str, Any]]:
    """生成单个组合的清仓/建仓指令列表。"""
    if cube_percent <= 0 or total_assets <= 0:
        return []
    holdings = _fetch_cube_current_holdings(cube_symbol)
    if not holdings:
        print(f"  ⚠️ {cube_symbol}({cube_name}) 未获取到当前持仓，跳过")
        return []

    ops: List[Dict[str, Any]] = []
    for h in holdings:
        price = h["price"]
        if price <= 0:
            print(f"  ⚠️ {h['stock_code']} 无价格，跳过")
            continue
        target_value = total_assets * cube_percent * h["weight"]
        raw_qty = int(target_value / price)
        if action == "sell":
            current = int(current_account_positions.get(h["stock_code"], 0))
            qty = min(raw_qty, current)
            qty = _round_lot(h["stock_code"], qty, "sell", current)
        else:
            qty = _round_lot(h["stock_code"], raw_qty, "buy")
        if qty <= 0:
            continue
        ops.append({
            "action": action,
            "code": h["stock_code"],
            "name": h["stock_name"],
            "quantity": qty,
            "price_type": "market",
        })
    return ops


# ---------- xueqiu_follower/config.py 的 cubes 字段写回 ----------

_CUBES_BLOCK_RE = re.compile(
    r'(\"cubes\"\s*:\s*)\[.*?\](?=\s*,\s*\n\s*\"mock_cubes\")',
    re.S,
)


def _format_cubes_block(cubes: List[Dict[str, Any]]) -> str:
    lines = ["["]
    for c in cubes:
        lines.append("        {")
        lines.append(f'            "cube": "{c["cube"]}",')
        lines.append(f'            "name": "{c["name"]}",')
        lines.append(f'            "percent": {c["percent"]},')
        lines.append(f'            "price_type": "{c["price_type"]}"')
        if c.get("fixed"):
            lines.append('            "fixed": True')
        lines.append("        },")
    lines.append("    ]")
    return "\n".join(lines)


def _is_fixed(cube: Dict[str, Any]) -> bool:
    return bool(cube.get("fixed"))


def _merge_with_fixed_cubes(
    choosen_cubes: List[Dict[str, Any]],
    old_cubes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """fixed=True：留在跟踪列表且 percent 不变（调仓仍由 follower 监控），再追加 choosen 中非重复项。"""
    fixed = [dict(c) for c in old_cubes if _is_fixed(c)]
    fixed_codes = {c["cube"] for c in fixed}
    merged = list(fixed)
    for c in choosen_cubes:
        if c["cube"] in fixed_codes:
            continue
        merged.append(c)
    return merged


def _write_follower_config(new_cubes: List[Dict[str, Any]]) -> bool:
    if not FOLLOWER_CONFIG_PATH.exists():
        print(f"❌ 找不到 follower 配置: {FOLLOWER_CONFIG_PATH}")
        return False

    text = FOLLOWER_CONFIG_PATH.read_text(encoding="utf-8")
    new_block = _format_cubes_block(new_cubes)
    if not _CUBES_BLOCK_RE.search(text):
        print("❌ 无法在 follower config.py 中定位 cubes 块")
        return False
    new_text = _CUBES_BLOCK_RE.sub(lambda m: m.group(1) + new_block, text)
    FOLLOWER_CONFIG_PATH.write_text(new_text, encoding="utf-8")
    print(f"✅ 已更新 {FOLLOWER_CONFIG_PATH}")
    return True


def _read_follower_old_cubes() -> List[Dict[str, Any]]:
    """用 ast.literal_eval 从 config.py 抠出 cubes 列表。"""
    if not FOLLOWER_CONFIG_PATH.exists():
        return []
    text = FOLLOWER_CONFIG_PATH.read_text(encoding="utf-8")
    m = _CUBES_BLOCK_RE.search(text)
    if not m:
        return []
    block = m.group(0).split(":", 1)[1].strip()
    try:
        return ast.literal_eval(block)
    except Exception as e:
        print(f"⚠️ 解析 follower 旧 cubes 失败: {e}")
        return []


# ---------- 主入口 ----------

def sync_choosen_to_follower(new_choosen_df: pd.DataFrame) -> Dict[str, Any]:
    """根据新的 choosen 表更新 follower 配置并生成 position_sync 指令。

    返回结果字典，便于上层服务/通知使用。
    """
    result: Dict[str, Any] = {
        "ok": False,
        "new_cubes": [],
        "added": [],     # [{cube, name, percent}]
        "removed": [],   # [{cube, name, old_percent}]
        "command_file": None,
        "operations_count": 0,
        "errors": [],
    }

    if new_choosen_df is None or new_choosen_df.empty:
        result["errors"].append("new_choosen_df 为空")
        return result

    old_cubes = _read_follower_old_cubes()
    old_map = {c["cube"]: c for c in old_cubes}

    # 1. 从 choosen 组装候选 cubes（前5跟踪、第6观察）
    rows = new_choosen_df.head(TOP_N_TRADE + 1).reset_index(drop=True)
    choosen_cubes: List[Dict[str, Any]] = []
    for i, row in rows.iterrows():
        percent = TRADE_PERCENT if i < TOP_N_TRADE else WATCH_PERCENT
        choosen_cubes.append({
            "cube": str(row["代码"]).strip().upper(),
            "name": str(row["名字"]).strip(),
            "percent": percent,
            "price_type": DEFAULT_PRICE_TYPE,
        })

    # 2. 合并 fixed 组合（不移出、不改 percent）
    new_cubes = _merge_with_fixed_cubes(choosen_cubes, old_cubes)
    result["new_cubes"] = new_cubes
    result["fixed_preserved"] = [
        {"cube": c["cube"], "name": c["name"], "percent": c["percent"]}
        for c in new_cubes if _is_fixed(c)
    ]

    new_map = {c["cube"]: c for c in new_cubes}

    # 3. 对比 old vs new（跳过 fixed 组合）
    removed: List[Tuple[str, str, float]] = []
    for code, oc in old_map.items():
        if _is_fixed(oc):
            continue
        op = float(oc.get("percent") or 0)
        np = float((new_map.get(code) or {}).get("percent") or 0)
        if op > 0 and np <= 0:
            removed.append((code, oc.get("name", code), op))

    added: List[Tuple[str, str, float]] = []
    for code, nc in new_map.items():
        if _is_fixed(nc):
            continue
        np = float(nc.get("percent") or 0)
        op = float((old_map.get(code) or {}).get("percent") or 0)
        if np > 0 and op <= 0:
            added.append((code, nc.get("name", code), np))

    result["removed"] = [{"cube": c, "name": n, "old_percent": p} for c, n, p in removed]
    result["added"] = [{"cube": c, "name": n, "percent": p} for c, n, p in added]

    # 3. 生成清仓/建仓指令
    all_ops: List[Dict[str, Any]] = []
    if removed or added:
        try:
            client = QmtClient()
            if not client.connect():
                result["errors"].append("无法连接 qmt_proxy，跳过指令生成")
            else:
                acc = client.account_info() or {}
                total_assets = float(acc.get("total_asset") or 0)
                cur_positions_list = client.positions()
                cur_positions = {p["code"]: p["volume"] for p in cur_positions_list}

                if total_assets <= 0:
                    result["errors"].append("total_assets=0，跳过指令生成")
                else:
                    print(f"账户总资产: {total_assets:,.2f}")
                    for code, name, old_pct in removed:
                        ops = _ops_for_cube(code, name, "sell", old_pct,
                                            total_assets, cur_positions)
                        print(f"  清仓 {name}({code}) 生成 {len(ops)} 条 SELL")
                        all_ops.extend(ops)

                    for code, name, new_pct in added:
                        ops = _ops_for_cube(code, name, "buy", new_pct,
                                            total_assets, cur_positions)
                        print(f"  建仓 {name}({code}) 生成 {len(ops)} 条 BUY")
                        all_ops.extend(ops)
                client.close()
        except Exception as e:
            result["errors"].append(f"指令生成异常: {e}")

    # 4. 写入指令文件
    if all_ops:
        source = (
            f"cube_analyst:update_choosen "
            f"+{len(added)}/-{len(removed)} "
            f"@{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        path = write_command_file(
            all_ops, source=source,
            note="由 update_choosen.py 自动生成的清仓/建仓指令",
        )
        if path:
            result["command_file"] = str(path)
            result["operations_count"] = len(all_ops)
            print(f"✅ 已写入指令文件 {path}")
    else:
        print("ℹ️ 无需生成任何清仓/建仓指令")

    # 5. 写回 follower config.py
    if not _write_follower_config(new_cubes):
        result["errors"].append("写入 follower config.py 失败")
        return result

    result["ok"] = True
    return result
