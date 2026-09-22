"""组合分析结果 SQLite 存储：月度统计、基础指标、汇总数据。"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

DB_PATH = Path(__file__).resolve().parent / "data" / "cube_analytics.db"

MONTHLY_COLUMNS = [
    "year_month",
    "month_start_value",
    "month_end_value",
    "monthly_change",
    "avg_daily_change",
    "max_daily_change",
    "max_daily_drawdown",
    "max_continuous_gain",
    "max_continuous_drawdown",
    "max_value",
    "min_value",
    "amplitude",
    "up_days",
    "down_days",
]

METRICS_COLUMNS = [
    "cube_symbol",
    "name",
    "link",
    "analyzed_date",
    "updated_at",
    "total_return",
    "simulated_return",
    "total_months",
    "total_days",
    "monthly_avg_change",
    "recent_monthly_avg_change",
    "daily_rebalancing_rate",
    "rebalancing_interval",
    "rebalancing_return_rate",
    "recent_return",
    "total_rebalancing_count",
    "recent_rebalancing_count",
    "recent_trading_days",
    "recent_max_monthly_gain",
    "recent_max_monthly_drawdown",
    "recent_max_continuous_monthly_gain",
    "recent_max_continuous_monthly_gain_months",
    "recent_max_continuous_monthly_drawdown",
    "recent_max_continuous_monthly_drawdown_months",
    "recent_up_months",
    "recent_down_months",
    "max_monthly_gain",
    "max_monthly_drawdown",
    "max_continuous_monthly_gain",
    "max_continuous_monthly_gain_months",
    "max_continuous_monthly_drawdown",
    "max_continuous_monthly_drawdown_months",
    "up_months",
    "down_months",
    "last_monthly_change",
    "last_rebalancing_date",
    "profitability_factor",
    "stability_factor",
    "efficiency_factor",
    "persistence_factor",
    "total_score",
]

SUMMARY_COLUMN_MAPPING = {
    "link": "组合链接",
    "name": "组合名称",
    "last_rebalancing_date": "最后调仓日期",
    "last_monthly_change": "最后月涨幅",
    "total_return": "总收益",
    "simulated_return": "模拟实仓收益率",
    "total_months": "交易月数",
    "monthly_avg_change": "月均涨幅",
    "recent_monthly_avg_change": "近年月均涨幅",
    "daily_rebalancing_rate": "日均调仓次数",
    "rebalancing_interval": "调仓间隔(自然日)",
    "rebalancing_return_rate": "每次调仓收益率",
    "recent_max_monthly_gain": "近年最大月涨幅",
    "recent_max_monthly_drawdown": "近年最大月回撤",
    "recent_max_continuous_monthly_gain": "近年最大连续涨幅",
    "recent_max_continuous_monthly_gain_months": "近年最大连续上涨月数",
    "recent_max_continuous_monthly_drawdown": "近年最大连续跌幅",
    "recent_max_continuous_monthly_drawdown_months": "近年最大连续下跌月数",
    "recent_up_months": "近年上涨月数",
    "recent_down_months": "近年下跌月数",
    "max_monthly_gain": "最大月涨幅",
    "max_monthly_drawdown": "最大月回撤",
    "max_continuous_monthly_gain": "最大连续涨幅",
    "max_continuous_monthly_gain_months": "最大连续上涨月数",
    "max_continuous_monthly_drawdown": "最大连续跌幅",
    "max_continuous_monthly_drawdown_months": "最大连续下跌月数",
    "up_months": "上涨月数",
    "down_months": "下跌月数",
    "profitability_factor": "盈利能力因子",
    "stability_factor": "稳定因子",
    "efficiency_factor": "交易效率因子",
    "persistence_factor": "持久因子",
    "total_score": "得分",
}

SUMMARY_COLUMNS_ORDER = list(SUMMARY_COLUMN_MAPPING.keys())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    monthly_cols = ",\n".join(
        f"{col} {'INTEGER' if col in ('up_days', 'down_days') else 'REAL'}"
        for col in MONTHLY_COLUMNS
    )
    metrics_cols = ",\n".join(
        f"{col} {'TEXT' if col in ('cube_symbol', 'name', 'link', 'analyzed_date', 'updated_at', 'last_rebalancing_date') else 'REAL'}"
        for col in METRICS_COLUMNS
    )
    with _connect() as conn:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS cube_monthly (
                cube_symbol TEXT NOT NULL,
                {monthly_cols},
                PRIMARY KEY (cube_symbol, year_month)
            );

            CREATE TABLE IF NOT EXISTS cube_metrics (
                {metrics_cols},
                PRIMARY KEY (cube_symbol)
            );

            CREATE TABLE IF NOT EXISTS summary_snapshot (
                run_date TEXT NOT NULL,
                cube_symbol TEXT NOT NULL,
                PRIMARY KEY (run_date, cube_symbol)
            );
            """
        )


def _record_to_metrics_row(record: Dict[str, Any], analyzed_date: str) -> Dict[str, Any]:
    symbol = record.get("symbol") or record.get("cube_symbol")
    row = {
        "cube_symbol": symbol,
        "name": record.get("name"),
        "link": record.get("link"),
        "analyzed_date": analyzed_date,
        "updated_at": _utc_now_iso(),
        "total_return": record.get("total_return", 0),
        "simulated_return": record.get("simulated_return", 0),
        "total_months": record.get("total_months", 0),
        "total_days": record.get("total_days", 0),
        "monthly_avg_change": record.get("monthly_avg_change", 0),
        "recent_monthly_avg_change": record.get("recent_monthly_avg_change", 0),
        "daily_rebalancing_rate": record.get("daily_rebalancing_rate", 0),
        "rebalancing_interval": record.get("rebalancing_interval"),
        "rebalancing_return_rate": record.get("rebalancing_return_rate", 0),
        "recent_return": record.get("recent_return", 0),
        "total_rebalancing_count": record.get("total_rebalancing_count", 0),
        "recent_rebalancing_count": record.get("recent_rebalancing_count", 0),
        "recent_trading_days": record.get("recent_trading_days", 0),
        "recent_max_monthly_gain": record.get("recent_max_monthly_gain", 0),
        "recent_max_monthly_drawdown": record.get("recent_max_monthly_drawdown", 0),
        "recent_max_continuous_monthly_gain": record.get("recent_max_continuous_monthly_gain", 0),
        "recent_max_continuous_monthly_gain_months": record.get("recent_max_continuous_monthly_gain_months", 0),
        "recent_max_continuous_monthly_drawdown": record.get("recent_max_continuous_monthly_drawdown", 0),
        "recent_max_continuous_monthly_drawdown_months": record.get("recent_max_continuous_monthly_drawdown_months", 0),
        "recent_up_months": record.get("recent_up_months", 0),
        "recent_down_months": record.get("recent_down_months", 0),
        "max_monthly_gain": record.get("max_monthly_gain", 0),
        "max_monthly_drawdown": record.get("max_monthly_drawdown", 0),
        "max_continuous_monthly_gain": record.get("max_continuous_monthly_gain", 0),
        "max_continuous_monthly_gain_months": record.get("max_continuous_monthly_gain_months", 0),
        "max_continuous_monthly_drawdown": record.get("max_continuous_monthly_drawdown", 0),
        "max_continuous_monthly_drawdown_months": record.get("max_continuous_monthly_drawdown_months", 0),
        "up_months": record.get("up_months", 0),
        "down_months": record.get("down_months", 0),
        "last_monthly_change": record.get("last_monthly_change", 0),
        "last_rebalancing_date": record.get("last_rebalancing_date"),
        "profitability_factor": record.get("profitability_factor", 0),
        "stability_factor": record.get("stability_factor", 0),
        "efficiency_factor": record.get("efficiency_factor", 0),
        "persistence_factor": record.get("persistence_factor", 0),
        "total_score": record.get("total_score", 0),
    }
    return row


def save_cube_analysis(
    record: Dict[str, Any],
    monthly_df: Optional[pd.DataFrame],
    analyzed_date: Optional[str] = None,
) -> None:
    """保存组合基础指标、因子与月度统计数据。"""
    init_db()
    analyzed_date = analyzed_date or datetime.now().strftime("%Y%m%d")
    symbol = record.get("symbol") or record.get("cube_symbol")
    if not symbol:
        raise ValueError("record 缺少 symbol")

    metrics_row = _record_to_metrics_row(record, analyzed_date)
    cols = [c for c in METRICS_COLUMNS]
    placeholders = ", ".join("?" for _ in cols)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "cube_symbol")

    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO cube_metrics ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT(cube_symbol) DO UPDATE SET {updates}
            """,
            [metrics_row[c] for c in cols],
        )

        conn.execute("DELETE FROM cube_monthly WHERE cube_symbol = ?", (symbol,))
        if monthly_df is not None and not monthly_df.empty:
            for _, row in monthly_df.iterrows():
                conn.execute(
                    f"""
                    INSERT INTO cube_monthly (cube_symbol, {", ".join(MONTHLY_COLUMNS)})
                    VALUES ({", ".join("?" for _ in range(len(MONTHLY_COLUMNS) + 1))})
                    """,
                    [symbol] + [row.get(col) for col in MONTHLY_COLUMNS],
                )

        conn.execute(
            """
            INSERT INTO summary_snapshot (run_date, cube_symbol)
            VALUES (?, ?)
            ON CONFLICT(run_date, cube_symbol) DO NOTHING
            """,
            (analyzed_date, symbol),
        )


def load_monthly_data(cube_symbol: str) -> pd.DataFrame:
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM cube_monthly
            WHERE cube_symbol = ?
            ORDER BY year_month
            """,
            (cube_symbol,),
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def load_metrics_record(cube_symbol: str) -> Optional[Dict[str, Any]]:
    init_db()
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM cube_metrics WHERE cube_symbol = ?",
            (cube_symbol,),
        ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["symbol"] = data["cube_symbol"]
    return data


def _metrics_rows_to_summary_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    rename = {k: v for k, v in SUMMARY_COLUMN_MAPPING.items() if k in df.columns}
    existing = [c for c in SUMMARY_COLUMNS_ORDER if c in df.columns]
    df = df[existing].rename(columns=rename)
    return df


def load_summary_dataframe(run_date: Optional[str] = None, symbols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    从 SQLite 加载汇总数据。

    run_date 默认今天；若指定 run_date 则只取该日 summary_snapshot 中的组合。
    """
    init_db()
    run_date = run_date or datetime.now().strftime("%Y%m%d")

    with _connect() as conn:
        if symbols:
            placeholders = ", ".join("?" for _ in symbols)
            query = f"""
                SELECT m.* FROM cube_metrics m
                WHERE m.cube_symbol IN ({placeholders})
            """
            rows = conn.execute(query, symbols).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT m.* FROM cube_metrics m
                INNER JOIN summary_snapshot s
                    ON m.cube_symbol = s.cube_symbol AND s.run_date = ?
                ORDER BY m.total_score DESC
                """,
                (run_date,),
            ).fetchall()

    records = []
    for row in rows:
        data = dict(row)
        data["symbol"] = data["cube_symbol"]
        records.append(data)
    return _metrics_rows_to_summary_df(records)


def register_summary_run(run_date: str, symbols: List[str]) -> None:
    """登记某日汇总包含的组合（分析完成后自动调用，也可手动补登记）。"""
    init_db()
    with _connect() as conn:
        for symbol in symbols:
            conn.execute(
                """
                INSERT INTO summary_snapshot (run_date, cube_symbol)
                VALUES (?, ?)
                ON CONFLICT(run_date, cube_symbol) DO NOTHING
                """,
                (run_date, symbol),
            )


def list_analyzed_symbols(run_date: Optional[str] = None) -> List[str]:
    init_db()
    run_date = run_date or datetime.now().strftime("%Y%m%d")
    with _connect() as conn:
        rows = conn.execute(
            "SELECT cube_symbol FROM summary_snapshot WHERE run_date = ? ORDER BY cube_symbol",
            (run_date,),
        ).fetchall()
    return [r["cube_symbol"] for r in rows]
