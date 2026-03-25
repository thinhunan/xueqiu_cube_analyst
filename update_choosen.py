#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动更新候选组合列表
1. 调用年榜、月榜分析获取候选组合
2. 与现有 choosen.csv 比较
3. 得分高于现有组合的添加，排名掉出前N的移除
4. 保存新列表，原列表备份为 history_{date}.csv
"""

import pandas as pd
import os
import shutil
from datetime import datetime
from data_analyst import generate_report, generate_summary_report, parse_csv_report
from data_loader import load_annual_rank_data, load_monthly_rank_data
from config import CUBE_LINK_URL
import re
import glob


def get_rank_cubes():
    """
    获取年榜和月榜的组合代码列表
    
    Returns:
        set: 组合代码集合
    """
    cubes = set()
    
    # 获取年榜数据
    print("=" * 60)
    print("获取年榜数据...")
    annual_data = load_annual_rank_data()
    if annual_data and 'list' in annual_data:
        for cube in annual_data['list']:
            symbol = cube.get('symbol', '')
            if symbol:
                cubes.add(symbol)
        print(f"年榜获取 {len(annual_data['list'])} 个组合")
    
    # 获取月榜数据
    print("=" * 60)
    print("获取月榜数据...")
    monthly_data = load_monthly_rank_data()
    if monthly_data and 'list' in monthly_data:
        for cube in monthly_data['list']:
            symbol = cube.get('symbol', '')
            if symbol:
                cubes.add(symbol)
        print(f"月榜获取 {len(monthly_data['list'])} 个组合")
    
    print(f"\n总计获取 {len(cubes)} 个唯一组合")
    return cubes


def analyze_cubes(cube_symbols):
    """
    批量分析组合
    
    Args:
        cube_symbols (set): 组合代码集合
        
    Returns:
        list: 成功分析的组合列表
    """
    success_symbols = []
    total = len(cube_symbols)
    
    print("=" * 60)
    print(f"开始批量分析 {total} 个组合...")
    print("=" * 60)
    
    for i, symbol in enumerate(cube_symbols, 1):
        print(f"\n[{i}/{total}] 分析: {symbol}")
        result = generate_report(symbol)
        if result and result != "SKIP":
            success_symbols.append(symbol)
    
    print(f"\n成功分析 {len(success_symbols)}/{total} 个组合")
    return success_symbols


def load_choosen_data(csv_path='choosen/choosen.csv'):
    """
    读取现有的候选组合数据
    
    Args:
        csv_path (str): CSV文件路径
        
    Returns:
        pd.DataFrame: 现有数据，如果不存在返回空DataFrame
    """
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        print(f"读取现有候选组合: {len(df)} 个")
        return df
    else:
        print("现有候选组合文件不存在，将创建新文件")
        return pd.DataFrame()


def load_latest_summary():
    """
    读取最新的汇总报表
    
    Returns:
        pd.DataFrame: 汇总数据
    """
    # 查找今天日期目录下的汇总报表
    today = datetime.now().strftime('%Y%m%d')
    summary_pattern = f"report/{today}/summary_*.xlsx"
    
    summary_files = glob.glob(summary_pattern)
    
    if not summary_files:
        print(f"未找到今天的汇总报表: {summary_pattern}")
        return None
    
    # 取最新的汇总报表
    latest_file = max(summary_files)
    print(f"读取汇总报表: {latest_file}")
    
    df = pd.read_excel(latest_file)
    return df


def extract_code(link):
    """从链接提取代码"""
    match = re.search(r'(ZH|SP)\d+', str(link))
    return match.group(0) if match else ''


def merge_and_filter(choosen_df, summary_df, top_n=6):
    """
    合并并筛选候选组合
    
    Args:
        choosen_df (pd.DataFrame): 现有候选组合
        summary_df (pd.DataFrame): 新分析的汇总数据
        top_n (int): 保留前N个组合
        
    Returns:
        pd.DataFrame: 新的候选组合列表
    """
    # 将汇总数据转换为 choosen 格式
    new_data = pd.DataFrame()
    new_data['名字'] = summary_df['组合名称']
    new_data['代码'] = summary_df['组合链接'].apply(extract_code)
    new_data['持续月数'] = summary_df['交易月数']
    new_data['净值'] = summary_df['总收益']
    new_data['实仓净值'] = summary_df['模拟实仓收益率']
    new_data['月均涨幅'] = summary_df['月均涨幅']
    new_data['近年月均涨幅'] = summary_df['近年月均涨幅']
    new_data['近月涨幅'] = summary_df['最后月涨幅']
    new_data['调仓间隔'] = summary_df['调仓间隔(自然日)']
    new_data['调仓收益率'] = summary_df['每次调仓收益率']
    new_data['最大回撤'] = summary_df['最大月回撤']
    new_data['盈利能力因子'] = summary_df['盈利能力因子']
    new_data['稳定因子'] = summary_df['稳定因子']
    new_data['交易效率因子'] = summary_df['交易效率因子']
    new_data['持久因子'] = summary_df['持久因子']
    new_data['得分'] = summary_df['得分']
    
    # 如果现有数据为空，直接返回新数据的前 top_n
    if choosen_df.empty:
        result = new_data.sort_values('得分', ascending=False).head(top_n)
        print(f"现有数据为空，返回得分最高的 {len(result)} 个组合")
        return result
    
    # 获取现有组合的最低得分
    existing_min_score = choosen_df['得分'].min()
    print(f"现有组合最低得分: {existing_min_score:.2f}")
    
    # 筛选得分高于现有最低分的新组合
    high_score_new = new_data[new_data['得分'] > existing_min_score].copy()
    print(f"得分高于现有最低分的新组合: {len(high_score_new)} 个")
    
    # 合并现有组合和新高分组合
    combined = pd.concat([choosen_df, high_score_new], ignore_index=True)
    
    # 去重（按代码），保留得分高的
    combined = combined.sort_values('得分', ascending=False).drop_duplicates(subset=['代码'], keep='first')
    
    # 按得分排序，取前 top_n
    result = combined.sort_values('得分', ascending=False).head(top_n)
    
    print(f"合并后共 {len(combined)} 个组合，筛选后保留前 {len(result)} 个")
    
    return result


def backup_and_save(old_df, new_df, csv_path='choosen/choosen.csv'):
    """
    备份原列表并保存新列表
    
    Args:
        old_df (pd.DataFrame): 原数据
        new_df (pd.DataFrame): 新数据
        csv_path (str): 目标文件路径
    """
    # 确保 choosen 目录存在
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    # 备份原文件
    if not old_df.empty and os.path.exists(csv_path):
        date_str = datetime.now().strftime('%Y%m%d')
        backup_path = f"choosen/history_{date_str}.csv"
        
        # 如果备份文件已存在，添加时间戳
        if os.path.exists(backup_path):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"choosen/history_{timestamp}.csv"
        
        shutil.copy(csv_path, backup_path)
        print(f"原列表已备份: {backup_path}")
    
    # 保存新列表
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
    更新现有候选组合的数据
    
    Args:
        choosen_df (pd.DataFrame): 现有候选组合
        
    Returns:
        pd.DataFrame: 更新后的数据
    """
    if choosen_df.empty:
        print("现有候选组合为空，跳过更新")
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
            print(f"更新失败")
    
    print("\n现有组合数据更新完成")
    return choosen_df


def main():
    """
    主函数：自动更新候选组合列表
    """
    print("=" * 60)
    print("自动更新候选组合列表")
    print("=" * 60)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 读取现有候选组合
    old_choosen = load_choosen_data()
    
    # 2. 先更新现有候选组合的数据
    if not old_choosen.empty:
        update_existing_choosen(old_choosen)
    
    # 3. 获取年榜和月榜组合
    cube_symbols = get_rank_cubes()
    
    if not cube_symbols:
        print("未获取到任何组合数据，退出")
        return
    
    # 4. 批量分析榜单组合
    analyze_cubes(cube_symbols)
    
    # 5. 生成汇总报表
    print("\n" + "=" * 60)
    print("生成汇总报表...")
    summary_result = generate_summary_report()
    
    if not summary_result:
        print("汇总报表生成失败，退出")
        return
    
    # 6. 读取汇总报表
    summary_df = load_latest_summary()
    
    if summary_df is None or summary_df.empty:
        print("汇总报表为空，退出")
        return
    
    # 7. 合并并筛选
    new_choosen = merge_and_filter(old_choosen, summary_df)
    
    # 8. 打印对比
    print_comparison(old_choosen, new_choosen)
    
    # 9. 备份并保存
    backup_and_save(old_choosen, new_choosen)
    
    print("\n" + "=" * 60)
    print("更新完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
