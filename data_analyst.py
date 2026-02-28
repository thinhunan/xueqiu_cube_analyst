import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import math
import glob
import re
from data_loader import load_cube_data, load_rebalancing_history
from config import CUBE_LINK_URL, TRADE_COST


def calculate_daily_changes(data):
    """
    计算每天的涨跌幅为当前value-上一个value
    
    Args:
        data (list): 原始数据列表
        
    Returns:
        pd.DataFrame: 包含计算后数据的DataFrame
    """
    if not data or len(data) == 0:
        return None
    
    # 取第一个symbol的数据
    first_symbol_data = data[0]
    symbol = first_symbol_data.get('symbol', '')
    name = first_symbol_data.get('name', '')
    daily_list = first_symbol_data.get('list', [])
    
    if not daily_list:
        return None
    
    # 转换为DataFrame
    df = pd.DataFrame(daily_list)
    
    # 转换时间戳为日期
    df['datetime'] = pd.to_datetime(df['time'], unit='ms')
    df['date'] = pd.to_datetime(df['date'])
    
    # 按日期排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算每天的涨跌幅为当前value-上一个value
    df['daily_change'] = 0.0
    for i in range(1, len(df)):
        prev_value = df.iloc[i-1]['value']
        curr_value = df.iloc[i]['value']
        df.loc[i, 'daily_change'] = curr_value - prev_value
    
    # 添加symbol和name信息
    df['symbol'] = symbol
    df['name'] = name
    
    return df


def calculate_monthly_metrics(df):
    """
    计算月度指标
    
    Args:
        df (pd.DataFrame): 包含daily_change的DataFrame
        
    Returns:
        dict: 包含各种指标的字典
    """
    if df is None or len(df) == 0:
        return {}
    
    # 添加年月列
    df['year_month'] = df['date'].dt.to_period('M')
    
    monthly_data = []
    
    # 按月分组计算
    for year_month, group in df.groupby('year_month'):
        if len(group) == 0:
            continue
            
        # 月初值和月末值
        month_start_value = group.iloc[0]['value']
        month_end_value = group.iloc[-1]['value']
        
        # 月涨幅：(月末值-月初值)/月初值
        monthly_change = (month_end_value - month_start_value) / month_start_value if month_start_value != 0 else 0
        
        # 平均日涨幅：(平均日变化)/月初值
        avg_daily_change = group['daily_change'].mean() / month_start_value if month_start_value != 0 else 0
        
        # 最大日涨幅：(最大日变化)/月初值
        max_daily_change = group['daily_change'].max() / month_start_value if month_start_value != 0 else 0
        
        # 最大日回撤：(负的最大变化)/月初值
        negative_changes = group[group['daily_change'] < 0]['daily_change']
        max_daily_drawdown = (negative_changes.min() / month_start_value) if len(negative_changes) > 0 and month_start_value != 0 else 0
        
        # 计算连续上涨的最大涨幅
        max_continuous_gain = 0
        current_gain = 0
        start_value = group.iloc[0]['value']
        
        for i, row in group.iterrows():
            if row['daily_change'] > 0:
                current_gain += row['daily_change']
                max_continuous_gain = max(max_continuous_gain, current_gain)
            else:
                current_gain = 0
                start_value = row['value']
        
        # 转换为百分比：(最大连续涨幅)/月初值
        max_continuous_gain = max_continuous_gain / month_start_value if month_start_value != 0 else 0
        
        # 计算连续下跌的最大回撤
        max_continuous_drawdown = 0
        current_drawdown = 0
        
        for i, row in group.iterrows():
            if row['daily_change'] < 0:
                current_drawdown += row['daily_change']
                max_continuous_drawdown = min(max_continuous_drawdown, current_drawdown)
            else:
                current_drawdown = 0
        
        # 转换为百分比：(最大连续回撤)/月初值
        max_continuous_drawdown = max_continuous_drawdown / month_start_value if month_start_value != 0 else 0
        
        # 最高值和最低值
        max_value = group['value'].max()
        min_value = group['value'].min()
        
        # 振幅：(最高值-最低值)/月初值
        amplitude = (max_value - min_value) / month_start_value if month_start_value != 0 else 0
        
        # 上涨天数和下跌天数
        up_days = len(group[group['daily_change'] > 0])
        down_days = len(group[group['daily_change'] < 0])
        
        monthly_data.append({
            'year_month': str(year_month),
            'month_start_value': month_start_value,
            'month_end_value': month_end_value,
            'monthly_change': monthly_change,
            'avg_daily_change': avg_daily_change,
            'max_daily_change': max_daily_change,
            'max_daily_drawdown': max_daily_drawdown,
            'max_continuous_gain': max_continuous_gain,
            'max_continuous_drawdown': max_continuous_drawdown,
            'max_value': max_value,
            'min_value': min_value,
            'amplitude': amplitude,
            'up_days': up_days,
            'down_days': down_days
        })
    
    # 转换为DataFrame
    monthly_df = pd.DataFrame(monthly_data)
    
    if monthly_df.empty:
        return {}
    
    # 计算统计指标
    total_days = len(df)
    total_months = len(monthly_df)
    
    # 月均涨跌幅
    monthly_avg_change = monthly_df['monthly_change'].mean()
    
    # 最大月涨幅
    max_monthly_gain = monthly_df['monthly_change'].max()
    
    # 最大月回撤
    max_monthly_drawdown = monthly_df['monthly_change'].min()
    
    # 计算连续上涨和下跌
    monthly_changes = monthly_df['monthly_change'].tolist()
    
    # 最大连续月上涨
    max_continuous_monthly_gain = 0
    max_continuous_monthly_gain_months = 0
    current_gain = 0
    current_months = 0
    
    for change in monthly_changes:
        if change > 0:
            current_gain += change
            current_months += 1
            if current_gain > max_continuous_monthly_gain:
                max_continuous_monthly_gain = current_gain
                max_continuous_monthly_gain_months = current_months
        else:
            current_gain = 0
            current_months = 0
    
    # 最大连续月下跌
    max_continuous_monthly_drawdown = 0
    max_continuous_monthly_drawdown_months = 0
    current_drawdown = 0
    current_months = 0
    
    for change in monthly_changes:
        if change < 0:
            current_drawdown += change
            current_months += 1
            if current_drawdown < max_continuous_monthly_drawdown:
                max_continuous_monthly_drawdown = current_drawdown
                max_continuous_monthly_drawdown_months = current_months
        else:
            current_drawdown = 0
            current_months = 0
    
    # 上涨月数和下跌月数
    up_months = len(monthly_df[monthly_df['monthly_change'] > 0])
    down_months = len(monthly_df[monthly_df['monthly_change'] < 0])
    
    # 计算最近一年的统计数据
    one_year_ago = datetime.now() - timedelta(days=365)
    recent_monthly_df = monthly_df.copy()
    recent_monthly_df['date'] = pd.to_datetime(recent_monthly_df['year_month'])
    recent_monthly_df = recent_monthly_df[recent_monthly_df['date'] >= one_year_ago]
    
    if len(recent_monthly_df) > 0:
        recent_monthly_avg_change = recent_monthly_df['monthly_change'].mean()
        recent_max_monthly_gain = recent_monthly_df['monthly_change'].max()
        recent_max_monthly_drawdown = recent_monthly_df['monthly_change'].min()
        
        # 计算最近一年的连续上涨和下跌
        recent_monthly_changes = recent_monthly_df['monthly_change'].tolist()
        
        # 最大连续月上涨
        recent_max_continuous_monthly_gain = 0
        recent_max_continuous_monthly_gain_months = 0
        current_gain = 0
        current_months = 0
        
        for change in recent_monthly_changes:
            if change > 0:
                current_gain += change
                current_months += 1
                if current_gain > recent_max_continuous_monthly_gain:
                    recent_max_continuous_monthly_gain = current_gain
                    recent_max_continuous_monthly_gain_months = current_months
            else:
                current_gain = 0
                current_months = 0
        
        # 最大连续月下跌
        recent_max_continuous_monthly_drawdown = 0
        recent_max_continuous_monthly_drawdown_months = 0
        current_drawdown = 0
        current_months = 0
        
        for change in recent_monthly_changes:
            if change < 0:
                current_drawdown += change
                current_months += 1
                if current_drawdown < recent_max_continuous_monthly_drawdown:
                    recent_max_continuous_monthly_drawdown = current_drawdown
                    recent_max_continuous_monthly_drawdown_months = current_months
            else:
                current_drawdown = 0
                current_months = 0
        
        # 最近一年上涨月数和下跌月数
        recent_up_months = len(recent_monthly_df[recent_monthly_df['monthly_change'] > 0])
        recent_down_months = len(recent_monthly_df[recent_monthly_df['monthly_change'] < 0])
    else:
        recent_monthly_avg_change = 0
        recent_max_monthly_gain = 0
        recent_max_monthly_drawdown = 0
        recent_max_continuous_monthly_gain = 0
        recent_max_continuous_monthly_gain_months = 0
        recent_max_continuous_monthly_drawdown = 0
        recent_max_continuous_monthly_drawdown_months = 0
        recent_up_months = 0
        recent_down_months = 0
    
    return {
        'monthly_data': monthly_df,
        'total_days': total_days,
        'total_months': total_months,
        'monthly_avg_change': monthly_avg_change,
        'max_monthly_gain': max_monthly_gain,
        'max_monthly_drawdown': max_monthly_drawdown,
        'max_continuous_monthly_gain': max_continuous_monthly_gain,
        'max_continuous_monthly_gain_months': max_continuous_monthly_gain_months,
        'max_continuous_monthly_drawdown': max_continuous_monthly_drawdown,
        'max_continuous_monthly_drawdown_months': max_continuous_monthly_drawdown_months,
        'up_months': up_months,
        'down_months': down_months,
        # 最近一年统计数据
        'recent_monthly_avg_change': recent_monthly_avg_change,
        'recent_max_monthly_gain': recent_max_monthly_gain,
        'recent_max_monthly_drawdown': recent_max_monthly_drawdown,
        'recent_max_continuous_monthly_gain': recent_max_continuous_monthly_gain,
        'recent_max_continuous_monthly_gain_months': recent_max_continuous_monthly_gain_months,
        'recent_max_continuous_monthly_drawdown': recent_max_continuous_monthly_drawdown,
        'recent_max_continuous_monthly_drawdown_months': recent_max_continuous_monthly_drawdown_months,
        'recent_up_months': recent_up_months,
        'recent_down_months': recent_down_months
    }


def calculate_rebalancing_return(df, history_data):
    """
    计算调仓收益率
    
    Args:
        df (pd.DataFrame): 包含daily_change的DataFrame
        history_data (dict): 调仓历史数据
        
    Returns:
        dict: 包含调仓相关指标的字典
    """
    if df is None or len(df) == 0:
        return {}
    
    # 如果history_data为空，返回空结果但保留结构
    if not history_data:
        return {
            'total_rebalancing_count': 0,
            'rebalancing_return_rate': 0,
            'last_rebalancing_date': None
        }
    
    # 提取totalCount（总调仓次数）
    total_count = history_data.get('totalCount', 0)
    
    # 提取最后调仓日期
    last_rebalancing_date = None
    list_data = history_data.get('list', [])
    if list_data and len(list_data) > 0:
        # 获取最后一次调仓记录（通常第一条是最新的）
        last_rebalancing = list_data[0]
        # 尝试从不同可能的字段中获取日期
        if 'created_at' in last_rebalancing:
            timestamp = last_rebalancing.get('created_at')
            if timestamp:
                try:
                    last_rebalancing_date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                except:
                    pass
        elif 'timestamp' in last_rebalancing:
            timestamp = last_rebalancing.get('timestamp')
            if timestamp:
                try:
                    last_rebalancing_date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                except:
                    pass
        elif 'date' in last_rebalancing:
            date_str = last_rebalancing.get('date')
            if date_str:
                try:
                    # 尝试解析日期字符串
                    if isinstance(date_str, (int, float)):
                        last_rebalancing_date = datetime.fromtimestamp(date_str / 1000).strftime('%Y-%m-%d')
                    else:
                        # 尝试直接解析日期字符串
                        date_obj = pd.to_datetime(date_str)
                        last_rebalancing_date = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
    
    if total_count == 0:
        return {
            'total_rebalancing_count': 0, 
            'rebalancing_return_rate': 0,
            'last_rebalancing_date': last_rebalancing_date
        }
    
    # 计算总收益：最后的value - 1
    final_value = df.iloc[-1]['value']
    total_return = final_value - 1.0
    
    # 计算每次调仓的收益率：(1+x)^totalCount = 1 + totalReturn
    # 求解 x = (1 + totalReturn)^(1/totalCount) - 1
    if total_count > 0 and total_return > -1:  # 确保总收益大于-100%
        rebalancing_return_rate = math.pow(1 + total_return, 1.0 / total_count) - 1
    else:
        rebalancing_return_rate = 0
    
    return {
        'total_rebalancing_count': total_count,
        'total_return': total_return,
        'rebalancing_return_rate': rebalancing_return_rate,
        'last_rebalancing_date': last_rebalancing_date
    }


def generate_report(cube_symbol, apply_skip_filters=True):
    """
    生成完整的分析报表

    Args:
        cube_symbol (str): 组合代码
        apply_skip_filters (bool): 是否应用收益率/月数等跳过条件；指定组合分析时为 False 则不跳过

    Returns:
        str: 报表文件路径，如果失败返回None，如果应该跳过返回"SKIP"
    """
    print(f"开始分析组合: {cube_symbol}")
    
    # 1. 加载数据
    data = load_cube_data(cube_symbol)
    if not data:
        print("数据加载失败")
        return None
    
    # 加载调仓历史数据
    history_data = load_rebalancing_history(cube_symbol)
    if not history_data:
        print("调仓历史数据加载失败，将使用空数据")
        history_data = {}  # 使用空字典而不是None，以便后续处理
    
    # 2. 计算每日变化
    df = calculate_daily_changes(data)
    if df is None:
        print("数据计算失败")
        return None
    
    # 3. 计算各种指标
    metrics = calculate_monthly_metrics(df)
    if not metrics:
        print("指标计算失败")
        return None
    
    # 4. 计算调仓收益率
    rebalancing_metrics = calculate_rebalancing_return(df, history_data)
    
    # 如果调仓历史数据为空但需要获取最后调仓日期，尝试重新加载
    if not rebalancing_metrics.get('last_rebalancing_date') and not history_data:
        try:
            history_data_retry = load_rebalancing_history(cube_symbol)
            if history_data_retry:
                # 只提取最后调仓日期
                list_data = history_data_retry.get('list', [])
                if list_data and len(list_data) > 0:
                    last_rebalancing = list_data[0]
                    if 'created_at' in last_rebalancing:
                        timestamp = last_rebalancing.get('created_at')
                        if timestamp:
                            try:
                                last_rebalancing_date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                                rebalancing_metrics['last_rebalancing_date'] = last_rebalancing_date
                                print(f"成功获取组合 {cube_symbol} 的最后调仓日期: {last_rebalancing_date}")
                            except Exception as e:
                                print(f"解析调仓日期时间戳失败: {e}")
        except Exception as e:
            print(f"重新获取调仓历史数据失败: {e}")
    
    # 5. 检查是否需要跳过（在计算完所有指标后进行判断）
    # 指定组合分析时(apply_skip_filters=False)不受收益率等限制，不进行跳过
    if apply_skip_filters:
        rebalancing_return_rate = rebalancing_metrics.get('rebalancing_return_rate', 0)
        total_rebalancing_count = rebalancing_metrics.get('total_rebalancing_count', 0)
        total_days = metrics.get('total_days', 0)
        total_months = metrics.get('total_months', 0)
        monthly_avg_change = metrics.get('monthly_avg_change', 0)

        # 检查条件1：已关停组合（数据为空或交易日数为0）
        if total_days == 0 or df is None or len(df) == 0:
            print(f"跳过组合 {cube_symbol}: 已关停组合或数据为空")
            return "SKIP"

        # 检查条件2：交易月数不足6个月
        if total_months < 6:
            print(f"跳过组合 {cube_symbol}: 交易月数不足6个月: {total_months}")
            return "SKIP"

        # 计算日均调仓次数
        daily_rebalancing_rate = total_rebalancing_count / total_days if total_days > 0 else 0

        # 检查条件3：日均调仓次数超过1
        if daily_rebalancing_rate > 1:
            print(f"跳过组合 {cube_symbol}: 日均调仓次数超过1: {daily_rebalancing_rate:.4f}")
            return "SKIP"

        # 检查条件4：模拟实盘收益为负
        if total_rebalancing_count > 0:
            simulated_return = math.pow(1 + rebalancing_return_rate - TRADE_COST, total_rebalancing_count) - 1
            if simulated_return < 0:
                print(f"跳过组合 {cube_symbol}: 模拟实盘收益为负: {simulated_return:.4%}")
                return "SKIP"

        # 检查条件5：总调仓次数大于交易日数
        if total_rebalancing_count > total_days:
            print(f"跳过组合 {cube_symbol}: 调仓次数({total_rebalancing_count})大于交易日数({total_days})")
            return "SKIP"

        # 检查条件6：月均涨跌幅小于4%
        if monthly_avg_change < 0.04:  # 4%
            print(f"跳过组合 {cube_symbol}: 月均涨跌幅小于4%: {monthly_avg_change:.4%}")
            return "SKIP"

    # 指定组合分析时若数据为空仍无法生成报表
    total_days = metrics.get('total_days', 0)
    if total_days == 0 or df is None or len(df) == 0:
        print(f"组合 {cube_symbol}: 已关停或数据为空，无法生成报表")
        return None
    
    # 4. 生成报表内容
    report_date = datetime.now().strftime('%Y%m%d')
    
    # 创建报表目录 - 使用日期子目录
    report_dir = os.path.join('report', report_date)
    if not os.path.exists(report_dir):
        os.makedirs(report_dir)
    
    # 生成CSV文件名
    csv_filename = f"{cube_symbol}_{report_date}.csv"
    csv_path = os.path.join(report_dir, csv_filename)
    
    # 准备报表数据
    report_lines = []
    
    # 添加月度数据表头
    report_lines.append([
        "年月", "月初值", "月末值", "月涨幅", "平均日涨幅", "最大日涨幅", 
        "最大日回撤", "最大涨幅", "最大回撤", "最高值", "最低值", "振幅", 
        "上涨天数", "下跌天数"
    ])
    
    # 添加月度数据
    if 'monthly_data' in metrics and not metrics['monthly_data'].empty:
        monthly_df = metrics['monthly_data']
        for _, row in monthly_df.iterrows():
            report_lines.append([
                str(row['year_month']),
                f"{row['month_start_value']:.4f}",
                f"{row['month_end_value']:.4f}",
                f"{row['monthly_change']:.4%}",
                f"{row['avg_daily_change']:.4%}",
                f"{row['max_daily_change']:.4%}",
                f"{row['max_daily_drawdown']:.4%}",
                f"{row['max_continuous_gain']:.4%}",
                f"{row['max_continuous_drawdown']:.4%}",
                f"{row['max_value']:.4f}",
                f"{row['min_value']:.4f}",
                f"{row['amplitude']:.4%}",
                str(row['up_days']),
                str(row['down_days'])
            ])
    
    # 添加统计数据
    report_lines.append([])  # 空行

    # 添加最近一年统计数据
    report_lines.append([
        "近一年月均涨跌幅","","", f"{metrics.get('recent_monthly_avg_change', 0):.4%}"
    ])
    report_lines.append([
        "近一年最大月涨幅", "","",f"{metrics.get('recent_max_monthly_gain', 0):.4%}"
    ])
    report_lines.append([
        "近一年最大月回撤", "","",f"{metrics.get('recent_max_monthly_drawdown', 0):.4%}"
    ])
    report_lines.append([
        "近一年最大连续月涨幅","","", f"{metrics.get('recent_max_continuous_monthly_gain', 0):.4%}"
    ])
    report_lines.append([
        "近一年最大连续上涨月数","","", str(metrics.get('recent_max_continuous_monthly_gain_months', 0))
    ])
    report_lines.append([
        "近一年最大连续月跌幅","","", f"{metrics.get('recent_max_continuous_monthly_drawdown', 0):.4%}"
    ])
    report_lines.append([
        "近一年最大连续下跌月数","","", str(metrics.get('recent_max_continuous_monthly_drawdown_months', 0))
    ])
    report_lines.append([
        "近一年上涨月数","","", str(metrics.get('recent_up_months', 0))
    ])
    report_lines.append([
        "近一年下跌月数","","", str(metrics.get('recent_down_months', 0))
    ])

    report_lines.append([])  # 空行

    report_lines.append([
        "交易日数", "","",str(metrics.get('total_days', 0))
    ])
    report_lines.append([
        "交易月数", "","",str(metrics.get('total_months', 0))
    ])
    report_lines.append([
        "月均涨跌幅","","", f"{metrics.get('monthly_avg_change', 0):.4%}"
    ])
    report_lines.append([
        "最大月涨幅","","", f"{metrics.get('max_monthly_gain', 0):.4%}"
    ])
    report_lines.append([
        "最大月回撤", "","",f"{metrics.get('max_monthly_drawdown', 0):.4%}"
    ])
    report_lines.append([
        "最大连续月上涨幅","","", f"{metrics.get('max_continuous_monthly_gain', 0):.4%}"
    ])
    report_lines.append([
        "最大连续上涨月数","","", str(metrics.get('max_continuous_monthly_gain_months', 0))
    ])
    report_lines.append([
        "最大连续月下跌幅", "","",f"{metrics.get('max_continuous_monthly_drawdown', 0):.4%}"
    ])
    report_lines.append([
        "最大连续下跌月数","","", str(metrics.get('max_continuous_monthly_drawdown_months', 0))
    ])
    report_lines.append([
        "上涨月数","","", str(metrics.get('up_months', 0))
    ])
    report_lines.append([
        "下跌月数", "","",str(metrics.get('down_months', 0))
    ])
    
    # 添加调仓相关数据
    report_lines.append([])  # 空行
    report_lines.append([
        "总调仓次数", "","",str(rebalancing_metrics.get('total_rebalancing_count', 0))
    ])
    report_lines.append([
        "总收益率", "","",f"{rebalancing_metrics.get('total_return', 0):.4%}"
    ])
    report_lines.append([
        "每次调仓收益率","","", f"{rebalancing_metrics.get('rebalancing_return_rate', 0):.6%}"
    ])
    
    # 计算模拟实仓收益率
    rebalancing_return_rate = rebalancing_metrics.get('rebalancing_return_rate', 0)
    total_rebalancing_count = rebalancing_metrics.get('total_rebalancing_count', 0)
    
    if total_rebalancing_count > 0:
        # 模拟实仓收益率 = (1 + 每次调仓收益率 - 交易成本)^总调仓次数 - 1
        simulated_return = math.pow(1 + rebalancing_return_rate - TRADE_COST, total_rebalancing_count) - 1
    else:
        simulated_return = 0
    
    report_lines.append([
        "模拟实仓收益率","","", f"{simulated_return:.4%}"
    ])
    
    # 添加最后调仓日期
    last_rebalancing_date = rebalancing_metrics.get('last_rebalancing_date', '')
    report_lines.append([
        "最后调仓日期","","", last_rebalancing_date if last_rebalancing_date else 'N/A'
    ])
    
    # 添加组合基本信息
    report_lines.append([])  # 空行
    report_lines.append([
        "组合代码","","", cube_symbol
    ])
    
    # 获取组合名称
    cube_name = "N/A"
    if data and len(data) > 0:
        cube_name = data[0].get('name', 'N/A')
    
    report_lines.append([
        "组合名称","","", cube_name
    ])
    
    # 生成组合链接
    cube_link = CUBE_LINK_URL.replace("<cube_symbol>", cube_symbol)
    report_lines.append([
        "组合链接","","", cube_link
    ])
    
    # 写入CSV文件
    try:
        with open(csv_path, 'w', encoding='utf-8-sig') as f:
            for line in report_lines:
                if isinstance(line, list):
                    f.write(','.join(map(str, line)) + '\n')
                else:
                    f.write(str(line) + '\n')
        
        print(f"报表已生成: {csv_path}")
        return csv_path
        
    except Exception as e:
        print(f"生成报表失败: {e}")
        return None


def parse_csv_report(file_path):
    """
    解析CSV报表文件，提取关键信息
    
    Args:
        file_path (str): CSV文件路径
        
    Returns:
        dict: 解析后的数据，如果解析失败返回None
    """
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
        
        # 初始化结果字典
        result = {}
        monthly_changes = []  # 存储月度涨幅数据用于计算稳定因子
        
        # 解析统计信息
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split(',')
            if len(parts) >= 4:
                key = parts[0].strip()
                value = parts[3].strip()
                
                # 解析各种指标
                if key == "组合代码":
                    result['symbol'] = value
                elif key == "组合名称":
                    result['name'] = value
                elif key == "组合链接":
                    result['link'] = value
                elif key == "总收益率":
                    result['total_return'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "模拟实仓收益率":
                    result['simulated_return'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "交易月数":
                    result['total_months'] = int(value) if value.isdigit() else 0
                elif key == "月均涨跌幅":
                    result['monthly_avg_change'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年月均涨跌幅":
                    result['recent_monthly_avg_change'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "总调仓次数":
                    result['total_rebalancing_count'] = int(value) if value.isdigit() else 0
                elif key == "交易日数":
                    result['total_days'] = int(value) if value.isdigit() else 0
                elif key == "每次调仓收益率":
                    result['rebalancing_return_rate'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年最大月涨幅":
                    result['recent_max_monthly_gain'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年最大月回撤":
                    result['recent_max_monthly_drawdown'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年最大连续月涨幅":
                    result['recent_max_continuous_monthly_gain'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年最大连续上涨月数":
                    result['recent_max_continuous_monthly_gain_months'] = int(value) if value.isdigit() else 0
                elif key == "近一年最大连续月跌幅":
                    result['recent_max_continuous_monthly_drawdown'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "近一年最大连续下跌月数":
                    result['recent_max_continuous_monthly_drawdown_months'] = int(value) if value.isdigit() else 0
                elif key == "近一年上涨月数":
                    result['recent_up_months'] = int(value) if value.isdigit() else 0
                elif key == "近一年下跌月数":
                    result['recent_down_months'] = int(value) if value.isdigit() else 0
                elif key == "最大月涨幅":
                    result['max_monthly_gain'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "最大月回撤":
                    result['max_monthly_drawdown'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "最大连续月上涨幅":
                    result['max_continuous_monthly_gain'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "最大连续上涨月数":
                    result['max_continuous_monthly_gain_months'] = int(value) if value.isdigit() else 0
                elif key == "最大连续月下跌幅":
                    result['max_continuous_monthly_drawdown'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "最大连续下跌月数":
                    result['max_continuous_monthly_drawdown_months'] = int(value) if value.isdigit() else 0
                elif key == "上涨月数":
                    result['up_months'] = int(value) if value.isdigit() else 0
                elif key == "下跌月数":
                    result['down_months'] = int(value) if value.isdigit() else 0
                elif key == "最后调仓日期":
                    result['last_rebalancing_date'] = value if value and value != 'N/A' else None
        
        # 解析月度数据用于计算稳定因子和最后月涨幅
        monthly_data_list = []  # 存储所有月度数据，用于找到最新的月份
        for line in lines:
            line = line.strip()
            if not line or line.startswith('年月,'):
                continue
                
            parts = line.split(',')
            if len(parts) >= 4 and parts[0] and parts[0] != '年月':
                try:
                    # 解析年月格式：YYYY-MM
                    year_month = parts[0].strip()
                    if len(year_month) == 7 and year_month[4] == '-':
                        monthly_change_str = parts[3].strip()
                        if monthly_change_str.endswith('%'):
                            monthly_change = float(monthly_change_str.replace('%', '')) / 100
                            monthly_changes.append(monthly_change)
                            # 保存年月和涨幅，用于找到最新的月份
                            monthly_data_list.append({
                                'year_month': year_month,
                                'monthly_change': monthly_change
                            })
                except:
                    continue
        
        # 找到最新的月份（按年月排序，取最后一个）
        last_monthly_change = None
        if monthly_data_list:
            # 按年月排序，确保获取最新的月份
            monthly_data_list.sort(key=lambda x: x['year_month'])
            last_monthly_change = monthly_data_list[-1]['monthly_change']
        
        # 保存最后月涨幅
        result['last_monthly_change'] = last_monthly_change if last_monthly_change is not None else 0
        
        # 计算日均调仓次数
        if result.get('total_rebalancing_count', 0) > 0 and result.get('total_days', 0) > 0:
            result['daily_rebalancing_rate'] = result['total_rebalancing_count'] / result['total_days']
        else:
            result['daily_rebalancing_rate'] = 0
        
        # 计算调仓间隔(自然日)：1 / 日均调仓次数 / 5 * 7
        if result.get('daily_rebalancing_rate', 0) > 0:
            result['rebalancing_interval'] = 1 / result['daily_rebalancing_rate'] / 5 * 7
        else:
            result['rebalancing_interval'] = None  # 如果没有调仓，间隔为None
        
        # 如果最后调仓日期不存在或为空，尝试从API重新获取
        if not result.get('last_rebalancing_date') or result.get('last_rebalancing_date') == 'N/A':
            symbol = result.get('symbol')
            if symbol:
                try:
                    from data_loader import load_rebalancing_history
                    history_data = load_rebalancing_history(symbol)
                    if history_data:
                        list_data = history_data.get('list', [])
                        if list_data and len(list_data) > 0:
                            last_rebalancing = list_data[0]
                            if 'created_at' in last_rebalancing:
                                timestamp = last_rebalancing.get('created_at')
                                if timestamp:
                                    try:
                                        last_rebalancing_date = datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
                                        result['last_rebalancing_date'] = last_rebalancing_date
                                        print(f"从API获取组合 {symbol} 的最后调仓日期: {last_rebalancing_date}")
                                    except Exception as e:
                                        print(f"解析组合 {symbol} 的调仓日期时间戳失败: {e}")
                except Exception as e:
                    print(f"获取组合 {symbol} 的调仓历史数据失败: {e}")
        
        # 计算新的因子
        result.update(calculate_factors(result, monthly_changes))
            
        return result
        
    except Exception as e:
        print(f"解析报表文件 {file_path} 失败: {e}")
        return None


def calculate_factors(data, monthly_changes):
    """
    计算各种因子
    
    Args:
        data (dict): 基础数据
        monthly_changes (list): 月度涨幅列表
        
    Returns:
        dict: 包含各种因子的字典
    """
    factors = {}
    
    # 1. 盈利能力因子：月均涨幅*8 + 近年月均涨幅*12
    monthly_avg = data.get('monthly_avg_change', 0)
    recent_monthly_avg = data.get('recent_monthly_avg_change', 0)
    factors['profitability_factor'] = monthly_avg * 8 + recent_monthly_avg * 12
    
    # 2. 稳定因子：基于月涨幅曲线的平滑度
    factors['stability_factor'] = calculate_stability_factor(monthly_changes)
    
    # 3. 交易效率因子：0.1 + 0.9/(1+EXP(-1.204119983*(LN(MIN(MAX(次均收益,0.0004),2))-LN(0.0158113883))))
    rebalancing_return_rate = data.get('rebalancing_return_rate', 0)
    # 限制次均收益在0.0004到2之间
    bounded_return = max(0.0004, min(rebalancing_return_rate, 2))
    if bounded_return > 0:
        log_return = math.log(bounded_return)
        log_threshold = math.log(0.0158113883)
        exp_term = math.exp(-1.204119983 * (log_return - log_threshold))
        factors['efficiency_factor'] = 0.1 + 0.9 / (1 + exp_term)
    else:
        factors['efficiency_factor'] = 0.1
    
    # 4. 持久因子：(LN(交易月数)-LN(1))/(LN(200)-LN(1))
    total_months = data.get('total_months', 0)
    if total_months > 0:
        factors['persistence_factor'] = (math.log(total_months) - math.log(1)) / (math.log(200) - math.log(1))
    else:
        factors['persistence_factor'] = 0
    
    # 5. 综合得分：盈利能力因子 + 持久因子*7 + 交易效率因子*3 + 稳定因子
    factors['total_score'] = (
        factors['profitability_factor'] + 
        factors['persistence_factor'] * 7 + 
        factors['efficiency_factor'] * 3 + 
        factors['stability_factor']
    )
    
    return factors


def calculate_stability_factor(monthly_changes):
    """
    计算稳定因子：基于月涨幅曲线的平滑度
    
    使用平滑权重衰减机制：
    - 最新数据权重为1.0
    - 每月递减0.01，最低保持0.4
    - 前60个月后权重不再减少
    
    Args:
        monthly_changes (list): 月度涨幅列表（从最新到最旧）
        
    Returns:
        float: 稳定因子值 (0-1之间，越平滑得分越高)
    """
    if len(monthly_changes) < 2:
        return 0
    
    # 计算权重：最新数据权重为1.0，每月递减0.01，最低保持0.4
    weights = []
    for i in range(len(monthly_changes)):
        # 权重衰减：1.0, 0.99, 0.98, ..., 0.4 (60个月后保持0.4)
        weight = max(0.4, 1.0 - i * 0.01)
        weights.append(weight)
    
    # 计算加权平均
    weighted_sum = sum(w * x for w, x in zip(weights, monthly_changes))
    total_weight = sum(weights)
    weighted_mean = weighted_sum / total_weight if total_weight > 0 else 0
    
    # 计算加权方差
    weighted_variance = sum(w * (x - weighted_mean) ** 2 for w, x in zip(weights, monthly_changes))
    weighted_variance = weighted_variance / total_weight if total_weight > 0 else 0
    
    # 计算加权标准差
    weighted_std_dev = math.sqrt(weighted_variance)
    
    # 稳定因子计算：使用改进的平滑度评估
    # 方法1：基于标准差的倒数，但加入平滑处理
    if weighted_std_dev > 0:
        # 使用对数变换来平滑极端值的影响
        log_std = math.log(1 + weighted_std_dev)
        stability_factor = 1 / (1 + log_std)
    else:
        stability_factor = 1.0
    
    # 方法2：基于变异系数的稳定性评估
    if abs(weighted_mean) > 0:
        coefficient_of_variation = weighted_std_dev / abs(weighted_mean)
        # 变异系数越小，稳定性越高
        cv_stability = 1 / (1 + coefficient_of_variation)
        # 结合两种方法，取平均值
        stability_factor = (stability_factor + cv_stability) / 2
    
    # 确保结果在0-1范围内
    stability_factor = max(0, min(1, stability_factor))
    
    return stability_factor


def generate_summary_report():
    """
    生成汇总报表
    
    Returns:
        str: 汇总文件路径，如果失败返回None
    """
    print("开始生成汇总报表...")
    
    # 获取当天日期目录下的CSV文件
    report_base_dir = 'report'
    if not os.path.exists(report_base_dir):
        print("报表目录不存在")
        return None
    
    # 获取当天日期
    today_date = datetime.now().strftime('%Y%m%d')
    today_dir = os.path.join(report_base_dir, today_date)
    
    # 只搜索当天日期目录下的CSV文件
    csv_files = []
    if os.path.exists(today_dir) and os.path.isdir(today_dir):
        date_csv_files = glob.glob(os.path.join(today_dir, '*.csv'))
        csv_files.extend(date_csv_files)
        print(f"搜索当天目录: {today_dir}")
    else:
        print(f"当天目录不存在: {today_dir}")
        return None
    
    if not csv_files:
        print(f"当天目录 {today_date} 下未找到任何报表文件")
        return None
    
    print(f"找到 {len(csv_files)} 个报表文件（仅当天目录）")
    
    # 按symbol分组，处理同一天可能有多个同名文件的情况
    symbol_files = {}
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        # 解析文件名格式：SYMBOL_YYYYMMDD.csv
        match = re.match(r'^([A-Z0-9]+)_(\d{8})\.csv$', filename)
        if match:
            symbol = match.group(1)
            date_str = match.group(2)
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            
            # 如果是同一天的文件，选择最新的（虽然通常只有一个）
            if symbol not in symbol_files or date_obj > symbol_files[symbol]['date']:
                symbol_files[symbol] = {
                    'file_path': file_path,
                    'date': date_obj,
                    'date_str': date_str
                }
    
    print(f"找到 {len(symbol_files)} 个唯一组合")
    
    # 解析所有报表
    summary_data = []
    for symbol, file_info in symbol_files.items():
        print(f"解析组合 {symbol} 的报表...")
        data = parse_csv_report(file_info['file_path'])
        if data:
            summary_data.append(data)
        else:
            print(f"跳过组合 {symbol}：解析失败")
    
    if not summary_data:
        print("没有成功解析任何报表")
        return None
    
    print(f"成功解析 {len(summary_data)} 个组合的报表")
    
    # 创建汇总DataFrame
    df = pd.DataFrame(summary_data)
    
    # 重新排列列的顺序
    columns_order = [
        'link', 'name', 'last_rebalancing_date', 'last_monthly_change', 'total_return', 'simulated_return', 'total_months', 
        'monthly_avg_change', 'recent_monthly_avg_change', 'daily_rebalancing_rate', 'rebalancing_interval',
        'rebalancing_return_rate', 'recent_max_monthly_gain', 'recent_max_monthly_drawdown',
        'recent_max_continuous_monthly_gain', 'recent_max_continuous_monthly_gain_months',
        'recent_max_continuous_monthly_drawdown', 'recent_max_continuous_monthly_drawdown_months',
        'recent_up_months', 'recent_down_months', 'max_monthly_gain', 'max_monthly_drawdown',
        'max_continuous_monthly_gain', 'max_continuous_monthly_gain_months',
        'max_continuous_monthly_drawdown', 'max_continuous_monthly_drawdown_months',
        'up_months', 'down_months', 'profitability_factor', 'stability_factor',
        'efficiency_factor', 'persistence_factor', 'total_score'
    ]
    
    # 只保留存在的列
    existing_columns = [col for col in columns_order if col in df.columns]
    df = df[existing_columns]
    
    # 重命名列为中文
    column_mapping = {
        'link': '组合链接',
        'name': '组合名称',
        'last_rebalancing_date': '最后调仓日期',
        'last_monthly_change': '最后月涨幅',
        'total_return': '总收益',
        'simulated_return': '模拟实仓收益率',
        'total_months': '交易月数',
        'monthly_avg_change': '月均涨幅',
        'recent_monthly_avg_change': '近年月均涨幅',
        'daily_rebalancing_rate': '日均调仓次数',
        'rebalancing_interval': '调仓间隔(自然日)',
        'rebalancing_return_rate': '每次调仓收益率',
        'recent_max_monthly_gain': '近年最大月涨幅',
        'recent_max_monthly_drawdown': '近年最大月回撤',
        'recent_max_continuous_monthly_gain': '近年最大连续涨幅',
        'recent_max_continuous_monthly_gain_months': '近年最大连续上涨月数',
        'recent_max_continuous_monthly_drawdown': '近年最大连续跌幅',
        'recent_max_continuous_monthly_drawdown_months': '近年最大连续下跌月数',
        'recent_up_months': '近年上涨月数',
        'recent_down_months': '近年下跌月数',
        'max_monthly_gain': '最大月涨幅',
        'max_monthly_drawdown': '最大月回撤',
        'max_continuous_monthly_gain': '最大连续涨幅',
        'max_continuous_monthly_gain_months': '最大连续上涨月数',
        'max_continuous_monthly_drawdown': '最大连续跌幅',
        'max_continuous_monthly_drawdown_months': '最大连续下跌月数',
        'up_months': '上涨月数',
        'down_months': '下跌月数',
        'profitability_factor': '盈利能力因子',
        'stability_factor': '稳定因子',
        'efficiency_factor': '交易效率因子',
        'persistence_factor': '持久因子',
        'total_score': '得分'
    }
    
    df = df.rename(columns=column_mapping)
    
    # 生成Excel文件 - 保存到当前日期的子目录
    summary_date = datetime.now().strftime('%Y%m%d')
    excel_filename = f"summary_{summary_date}.xlsx"
    
    # 创建当前日期的子目录
    current_report_dir = os.path.join(report_base_dir, summary_date)
    if not os.path.exists(current_report_dir):
        os.makedirs(current_report_dir)
    
    excel_path = os.path.join(current_report_dir, excel_filename)
    
    try:
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='汇总报表', index=False)
        
        print(f"汇总报表已生成: {excel_path}")
        print(f"包含 {len(df)} 个组合的数据")
        return excel_path
        
    except Exception as e:
        print(f"生成Excel文件失败: {e}")
        return None


if __name__ == "__main__":
    # 测试功能
    test_symbol = "ZH3186221"
    result = generate_report(test_symbol)
    if result:
        print(f"测试完成，报表保存在: {result}")
