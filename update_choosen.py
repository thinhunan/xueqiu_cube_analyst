#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动更新候选组合列表
1. 调用年榜、月榜分析获取候选组合
2. 查看 choosen/choosen.csv 里原有的选中组合
3. 新候选和老选中组合去重得出所有候选组合
4. 下载和计算所有候选组合的新数据
5. 得出得分最高的前6名，保存新列表，原列表备份为 history_{date}.csv
6. （可选）同步 xueqiu_follower/config.py 并生成 position_sync 指令
"""

# 是否自动同步跟单配置与 position_sync 指令（默认关闭）
ENABLE_FOLLOWER_SYNC = False

import argparse
import pandas as pd
import os
import shutil
from datetime import datetime
from typing import Any, Dict, Optional
from data_analyst import generate_report, generate_summary_report
from cube_store import load_summary_dataframe
from data_loader import load_annual_rank_data, load_monthly_rank_data
from sync_to_follower import sync_choosen_to_follower
import re
import glob


def get_rank_cubes():
    """
    获取年榜和月榜的组合代码列表

    Returns:
        set: 组合代码集合
    """
    cubes = set()

    print("=" * 60)
    print("获取年榜数据...")
    annual_data = load_annual_rank_data()
    if annual_data and 'list' in annual_data:
        for cube in annual_data['list']:
            symbol = cube.get('symbol', '')
            if symbol:
                cubes.add(symbol)
        print(f"年榜获取 {len(annual_data['list'])} 个组合")

    print("=" * 60)
    print("获取月榜数据...")
    monthly_data = load_monthly_rank_data()
    if monthly_data and 'list' in monthly_data:
        for cube in monthly_data['list']:
            symbol = cube.get('symbol', '')
            if symbol:
                cubes.add(symbol)
        print(f"月榜获取 {len(monthly_data['list'])} 个组合")

    print(f"\n年/月榜合计 {len(cubes)} 个唯一组合")
    return cubes


def load_choosen_data(csv_path='choosen/choosen.csv'):
    """
    读取现有的选中组合数据

    Args:
        csv_path (str): CSV文件路径

    Returns:
        pd.DataFrame: 现有数据，如果不存在返回空DataFrame
    """
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"读取现有选中组合: {len(df)} 个")
        return df
    else:
        print("现有选中组合文件不存在，将创建新文件")
        return pd.DataFrame()


def collect_candidate_symbols(rank_cubes, choosen_df):
    """
    合并年/月榜候选与既有选中组合代码并去重

    Args:
        rank_cubes (set): 年榜、月榜组合代码
        choosen_df (pd.DataFrame): 现有选中组合

    Returns:
        set: 去重后的全部候选组合代码
    """
    symbols = set(rank_cubes)
    old_count = 0

    if not choosen_df.empty and '代码' in choosen_df.columns:
        old_codes = (
            choosen_df['代码']
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .tolist()
        )
        old_count = len(old_codes)
        symbols.update(old_codes)

    print(f"榜单候选: {len(rank_cubes)} 个，原有选中: {old_count} 个，去重后合计: {len(symbols)} 个")
    return symbols


def analyze_cubes(cube_symbols, apply_skip_filters=False):
    """
    批量分析组合

    Args:
        cube_symbols (set | list): 组合代码集合
        apply_skip_filters (bool): 是否应用跳过过滤（更新流程默认不过滤，保证公平比较）

    Returns:
        list: 成功分析的组合列表
    """
    symbols = sorted(cube_symbols)
    success_symbols = []
    total = len(symbols)

    print("=" * 60)
    print(f"开始批量分析 {total} 个候选组合...")
    print("=" * 60)

    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{total}] 分析: {symbol}")
        result = generate_report(symbol, apply_skip_filters=apply_skip_filters)
        if result and result != "SKIP":
            success_symbols.append(symbol)

    print(f"\n成功分析 {len(success_symbols)}/{total} 个组合")
    return success_symbols


def load_latest_summary():
    """
    读取最新的汇总数据（优先 SQLite，回退 Excel）。

    Returns:
        pd.DataFrame: 汇总数据
    """
    today = datetime.now().strftime('%Y%m%d')
    df = load_summary_dataframe(today)
    if df is not None and not df.empty:
        print(f"从 SQLite 读取汇总数据: {len(df)} 个组合 (run_date={today})")
        return df

    summary_pattern = f"report/{today}/summary_*.xlsx"
    summary_files = glob.glob(summary_pattern)
    if not summary_files:
        print(f"未找到今天的汇总数据: SQLite 与 {summary_pattern} 均为空")
        return None

    latest_file = max(summary_files)
    print(f"SQLite 无数据，回退读取 Excel: {latest_file}")
    return pd.read_excel(latest_file)


def extract_code(link):
    """从链接提取代码"""
    match = re.search(r'(ZH|SP)\d+', str(link))
    return match.group(0) if match else ''


def summary_to_choosen(summary_df):
    """
    将汇总报表转换为 choosen.csv 格式

    Args:
        summary_df (pd.DataFrame): 汇总数据

    Returns:
        pd.DataFrame: choosen 格式数据
    """
    choosen = pd.DataFrame()
    choosen['名字'] = summary_df['组合名称']
    choosen['代码'] = summary_df['组合链接'].apply(extract_code)
    choosen['持续月数'] = summary_df['交易月数']
    choosen['净值'] = summary_df['总收益']
    choosen['实仓净值'] = summary_df['模拟实仓收益率']
    choosen['月均涨幅'] = summary_df['月均涨幅']
    choosen['近年月均涨幅'] = summary_df['近年月均涨幅']
    choosen['近月涨幅'] = summary_df['最后月涨幅']
    choosen['调仓间隔'] = summary_df['调仓间隔(自然日)']
    choosen['调仓收益率'] = summary_df['每次调仓收益率']
    choosen['最大回撤'] = summary_df['最大月回撤']
    choosen['盈利能力因子'] = summary_df['盈利能力因子']
    choosen['稳定因子'] = summary_df['稳定因子']
    choosen['交易效率因子'] = summary_df['交易效率因子']
    choosen['持久因子'] = summary_df['持久因子']
    choosen['得分'] = summary_df['得分']
    return choosen


def select_top_from_summary(summary_df, candidate_symbols=None, top_n=6):
    """
    从汇总数据中按得分选取前 N 名

    Args:
        summary_df (pd.DataFrame): 汇总数据
        candidate_symbols (set | None): 仅保留这些代码；None 表示不过滤
        top_n (int): 保留前 N 个组合

    Returns:
        pd.DataFrame: 得分最高的前 N 个组合
    """
    choosen = summary_to_choosen(summary_df)
    choosen = choosen[choosen['代码'].astype(bool)]

    if candidate_symbols:
        candidate_symbols = {str(c).strip().upper() for c in candidate_symbols}
        choosen = choosen[choosen['代码'].str.upper().isin(candidate_symbols)]

    choosen = (
        choosen.sort_values('得分', ascending=False)
        .drop_duplicates(subset=['代码'], keep='first')
        .head(top_n)
    )

    print(f"候选池中成功分析 {len(choosen)} 个组合，保留得分前 {top_n} 名（实际 {len(choosen)} 个）")
    return choosen


def backup_and_save(old_df, new_df, csv_path='choosen/choosen.csv'):
    """
    备份原列表并保存新列表

    Args:
        old_df (pd.DataFrame): 原数据
        new_df (pd.DataFrame): 新数据
        csv_path (str): 目标文件路径
    """
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    if not old_df.empty and os.path.exists(csv_path):
        date_str = datetime.now().strftime('%Y%m%d')
        backup_path = f"choosen/history_{date_str}.csv"

        if os.path.exists(backup_path):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"choosen/history_{timestamp}.csv"

        shutil.copy(csv_path, backup_path)
        print(f"原列表已备份: {backup_path}")

    new_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"新列表已保存: {csv_path}")


def print_comparison(old_df, new_df):
    """
    打印新旧列表对比

    Args:
        old_df (pd.DataFrame): 原数据
        new_df (pd.DataFrame): 新数据
    """
    print("\n" + "=" * 60)
    print("候选组合变更对比")
    print("=" * 60)

    if old_df.empty:
        print("原列表: 空")
    else:
        print(f"\n原列表 ({len(old_df)} 个):")
        for _, row in old_df.iterrows():
            print(f"  {row['名字']} ({row['代码']}): 得分 {row['得分']:.2f}")

    print(f"\n新列表 ({len(new_df)} 个):")
    for _, row in new_df.iterrows():
        status = ""
        if not old_df.empty:
            old_codes = set(old_df['代码'].tolist())
            if row['代码'] not in old_codes:
                status = " [新增]"
        print(f"  {row['名字']} ({row['代码']}): 得分 {row['得分']:.2f}{status}")

    if not old_df.empty:
        old_codes = set(old_df['代码'].tolist())
        new_codes = set(new_df['代码'].tolist())
        removed = old_codes - new_codes
        if removed:
            print(f"\n移除的组合: {', '.join(removed)}")


def update_existing_choosen(choosen_df):
    """
    更新现有选中组合的数据（供 analyst.py summary 等场景调用）

    Args:
        choosen_df (pd.DataFrame): 现有选中组合

    Returns:
        pd.DataFrame: 原 DataFrame（数据已刷新到 report 目录）
    """
    if choosen_df.empty:
        print("现有选中组合为空，跳过更新")
        return choosen_df

    codes = choosen_df['代码'].tolist()
    print(f"开始更新 {len(codes)} 个现有组合的数据...")
    print("=" * 60)

    for i, code in enumerate(codes, 1):
        print(f"\n[{i}/{len(codes)}] 更新: {code}")
        result = generate_report(code, apply_skip_filters=False)
        if result:
            print(f"更新成功: {result}")
        else:
            print("更新失败")

    print("\n现有组合数据更新完成")
    return choosen_df


def _build_result(status: str, message: str, **extra) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "status": status,                                 # ok | empty | error
        "message": message,
        "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "old_choosen": [],
        "new_choosen": [],
        "added_cubes": [],
        "removed_cubes": [],
        "command_file": None,
        "operations_count": 0,
        "sync_errors": [],
    }
    out.update(extra)
    return out


def main(enable_follower_sync: Optional[bool] = None) -> Dict[str, Any]:
    """主函数：自动更新候选组合列表，并返回结构化结果。

    Args:
        enable_follower_sync: 是否更新 xueqiu_follower/config.py 并生成 position_sync。
            None 时使用模块常量 ENABLE_FOLLOWER_SYNC（默认 False）。
    """
    if enable_follower_sync is None:
        enable_follower_sync = ENABLE_FOLLOWER_SYNC
    print("=" * 60)
    print("自动更新候选组合列表")
    print("=" * 60)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    rank_cubes = get_rank_cubes()
    old_choosen = load_choosen_data()

    all_candidates = collect_candidate_symbols(rank_cubes, old_choosen)
    if not all_candidates:
        msg = "未获取到任何候选组合"
        print(msg)
        return _build_result("empty", msg)

    analyze_cubes(all_candidates, apply_skip_filters=False)

    print("\n" + "=" * 60)
    print("生成汇总报表...")
    summary_result = generate_summary_report()
    if not summary_result:
        return _build_result("error", "汇总报表生成失败")

    summary_df = load_latest_summary()
    if summary_df is None or summary_df.empty:
        return _build_result("error", "汇总报表为空")

    new_choosen = select_top_from_summary(summary_df, all_candidates, top_n=6)
    if new_choosen.empty:
        return _build_result("error", "未能从汇总中选出任何组合")

    print_comparison(old_choosen, new_choosen)
    backup_and_save(old_choosen, new_choosen)

    old_list = (
        [{"code": r["代码"], "name": r["名字"], "score": float(r["得分"])}
         for _, r in old_choosen.iterrows()]
        if not old_choosen.empty else []
    )
    new_list = [
        {"code": r["代码"], "name": r["名字"], "score": float(r["得分"])}
        for _, r in new_choosen.iterrows()
    ]

    if enable_follower_sync:
        print("\n" + "=" * 60)
        print("同步到 xueqiu_follower 并生成 position_sync 指令...")
        sync_res = sync_choosen_to_follower(new_choosen)
        result = _build_result(
            "ok" if sync_res.get("ok") else "error",
            "更新完成" if sync_res.get("ok") else "选股完成但同步失败",
            old_choosen=old_list,
            new_choosen=new_list,
            added_cubes=sync_res.get("added", []),
            removed_cubes=sync_res.get("removed", []),
            command_file=sync_res.get("command_file"),
            operations_count=sync_res.get("operations_count", 0),
            sync_errors=sync_res.get("errors", []),
        )
    else:
        print("\n" + "=" * 60)
        print("已跳过 xueqiu_follower / position_sync 同步（ENABLE_FOLLOWER_SYNC=False）")
        print("如需开启：python update_choosen.py --sync-follower")
        result = _build_result(
            "ok",
            "更新完成（未同步 follower）",
            old_choosen=old_list,
            new_choosen=new_list,
            added_cubes=[],
            removed_cubes=[],
            command_file=None,
            operations_count=0,
            sync_errors=[],
        )

    print("\n" + "=" * 60)
    print(f"更新结果: {result['status']} — {result['message']}")
    print("=" * 60)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="自动更新候选组合列表")
    parser.add_argument(
        "--sync-follower",
        action="store_true",
        help="同步 xueqiu_follower/config.py 并生成 position_sync 指令（默认关闭）",
    )
    args = parser.parse_args()
    main(enable_follower_sync=args.sync_follower)
