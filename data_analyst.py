import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import math
import glob
import re
from data_loader import load_cube_data, load_rebalancing_history_with_retry
from config import CUBE_LINK_URL, TRADE_COST
from cube_store import load_summary_dataframe, save_cube_analysis

RECENT_YEAR_DAYS = 365


def get_one_year_ago():
    """与月度「近一年」统计一致的时间窗口起点"""
    return datetime.now() - timedelta(days=RECENT_YEAR_DAYS)


def parse_rebalancing_timestamp_ms(record):
    """从调仓记录解析毫秒时间戳"""
    for field in ('created_at', 'updated_at', 'timestamp'):
        ts = record.get(field)
        if ts is None or ts == '':
            continue
        try:
            return float(ts)
        except (TypeError, ValueError):
            continue
    date_val = record.get('date')
    if date_val is None or date_val == '':
        return None
    try:
        if isinstance(date_val, (int, float)):
            return float(date_val)
        return pd.to_datetime(date_val).timestamp() * 1000
    except (TypeError, ValueError):
        return None


def filter_recent_rebalancing_records(history_data):
    """筛选近一年内的调仓记录"""
    one_year_ago_ms = get_one_year_ago().timestamp() * 1000
    recent = []
    for item in history_data.get('list', []) or []:
        ts = parse_rebalancing_timestamp_ms(item)
        if ts is not None and ts >= one_year_ago_ms:
            recent.append(item)
    return recent


def calculate_recent_year_nav_metrics(df):
    """
    计算近一年净值收益与交易日数

    Returns:
        tuple: (近一年收益率, 近一年交易日数)
    """
    one_year_ago = get_one_year_ago()
    recent_df = df[df['date'] >= pd.Timestamp(one_year_ago)]
    if len(recent_df) == 0:
        recent_df = df
    if len(recent_df) == 0:
        return 0.0, 0

    start_value = recent_df.iloc[0]['value']
    end_value = recent_df.iloc[-1]['value']
    recent_return = (end_value / start_value - 1.0) if start_value > 0 else 0.0
    return recent_return, len(recent_df)


def calculate_rebalancing_trade_metrics(recent_count, recent_return):
    """根据近一年收益与调仓次数计算每次调仓收益率"""
    if recent_count > 0 and recent_return > -1:
        return math.pow(1 + recent_return, 1.0 / recent_count) - 1
    return 0.0


def calculate_daily_rebalancing_and_interval(recent_count, recent_trading_days):
    """计算近一年日均调仓次数与自然日调仓间隔"""
    if recent_trading_days > 0 and recent_count > 0:
        daily_rate = recent_count / recent_trading_days
        interval = 1 / daily_rate / 5 * 7
        return daily_rate, interval
    return 0.0, None


def estimate_rebalance_turnover_ratio(rec):
    """
    估算单次调仓成交额占组合净值比例。

    Σ|Δ权重| 同时计入买入与卖出两侧，实际成交额为其一半。
    """
    weight_delta = 0.0
    for item in rec.get('rebalancing_histories') or []:
        try:
            prev_weight = float(item.get('prev_weight') or item.get('prev_target_weight') or 0)
            target_weight = float(item.get('target_weight') or item.get('weight') or 0)
        except (TypeError, ValueError):
            continue
        weight_delta += abs(target_weight - prev_weight)
    return weight_delta / 200.0


def get_nav_at(df, ts_ms):
    """获取不晚于调仓时刻的最近净值"""
    if df is None or len(df) == 0 or ts_ms is None or ts_ms <= 0:
        return None
    target = pd.Timestamp(datetime.fromtimestamp(ts_ms / 1000))
    sub = df[df['date'] <= target]
    if sub.empty:
        return float(df.iloc[0]['value'])
    return float(sub.iloc[-1]['value'])


def calculate_simulated_return_with_turnover(df, history_data):
    """
    全历史模拟实仓收益：沿净值曲线走，每次调仓仅对换手金额扣 TRADE_COST。

    与总收益同口径（全历史），无调仓记录时退化为净值总收益。
    """
    if df is None or len(df) == 0:
        return 0.0

    records = history_data.get('list', []) or []
    df = df.sort_values('date').reset_index(drop=True)

    events = []
    for rec in records:
        ts = parse_rebalancing_timestamp_ms(rec)
        if ts is not None:
            events.append((ts, rec))
    events.sort(key=lambda x: x[0])

    if not events:
        start_nav = float(df.iloc[0]['value'])
        end_nav = float(df.iloc[-1]['value'])
        return (end_nav / start_nav - 1.0) if start_nav > 0 else 0.0

    equity = 1.0
    prev_nav = float(df.iloc[0]['value'])
    if prev_nav <= 0:
        return 0.0

    for ts, rec in events:
        curr_nav = get_nav_at(df, ts)
        if curr_nav is None or prev_nav <= 0:
            continue
        equity *= curr_nav / prev_nav
        turnover = estimate_rebalance_turnover_ratio(rec)
        equity *= max(0.0, 1.0 - turnover * TRADE_COST)
        prev_nav = curr_nav

    final_nav = float(df.iloc[-1]['value'])
    if prev_nav > 0:
        equity *= final_nav / prev_nav

    return equity - 1.0


def extract_last_rebalancing_date(history_data):
    """从调仓历史中提取最后一次调仓日期"""
    list_data = history_data.get('list', []) if history_data else []
    if not list_data:
        return None

    last_rebalancing = list_data[0]
    ts = parse_rebalancing_timestamp_ms(last_rebalancing)
    if ts is not None:
        try:
            return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')
        except (OSError, ValueError):
            pass

    for field in ('created_at', 'timestamp', 'date'):
        if field not in last_rebalancing:
            continue
        val = last_rebalancing.get(field)
        if not val:
            continue
        try:
            if isinstance(val, (int, float)):
                return datetime.fromtimestamp(val / 1000).strftime('%Y-%m-%d')
            return pd.to_datetime(val).strftime('%Y-%m-%d')
        except (TypeError, ValueError, OSError):
            continue
    return None


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
    one_year_ago = get_one_year_ago()
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
    计算调仓相关指标

    - 模拟实仓收益率：全历史净值 + 按换手金额扣费（对应总收益）
    - 日均调仓次数 / 调仓间隔 / 每次调仓收益率：近一年口径
    """
    empty_metrics = {
        'total_rebalancing_count': 0,
        'recent_rebalancing_count': 0,
        'recent_trading_days': 0,
        'total_return': 0,
        'recent_return': 0,
        'rebalancing_return_rate': 0,
        'simulated_return': 0,
        'daily_rebalancing_rate': 0,
        'rebalancing_interval': None,
        'last_rebalancing_date': None,
    }

    if df is None or len(df) == 0:
        return empty_metrics.copy()

    history_data = history_data or {}
    total_count = history_data.get('totalCount', 0)
    last_rebalancing_date = extract_last_rebalancing_date(history_data)

    final_value = df.iloc[-1]['value']
    total_return = final_value - 1.0

    recent_records = filter_recent_rebalancing_records(history_data)
    recent_count = len(recent_records)
    recent_return, recent_trading_days = calculate_recent_year_nav_metrics(df)
    rebalancing_return_rate = calculate_rebalancing_trade_metrics(recent_count, recent_return)
    daily_rebalancing_rate, rebalancing_interval = calculate_daily_rebalancing_and_interval(
        recent_count, recent_trading_days
    )
    simulated_return = calculate_simulated_return_with_turnover(df, history_data)

    return {
        'total_rebalancing_count': total_count,
        'recent_rebalancing_count': recent_count,
        'recent_trading_days': recent_trading_days,
        'total_return': total_return,
        'recent_return': recent_return,
        'rebalancing_return_rate': rebalancing_return_rate,
        'simulated_return': simulated_return,
        'daily_rebalancing_rate': daily_rebalancing_rate,
        'rebalancing_interval': rebalancing_interval,
        'last_rebalancing_date': last_rebalancing_date,
    }


def build_analysis_record(cube_symbol, cube_name, metrics, rebalancing_metrics):
    """将分析结果组装为汇总/入库用的结构化记录（含因子）。"""
    monthly_df = metrics.get("monthly_data")
    monthly_changes = []
    last_monthly_change = 0.0
    if monthly_df is not None and not monthly_df.empty:
        sorted_df = monthly_df.sort_values("year_month")
        monthly_changes = sorted_df["monthly_change"].tolist()
        last_monthly_change = float(sorted_df.iloc[-1]["monthly_change"])

    record = {
        "symbol": cube_symbol,
        "name": cube_name,
        "link": CUBE_LINK_URL.replace("<cube_symbol>", cube_symbol),
        "total_return": rebalancing_metrics.get("total_return", 0),
        "simulated_return": rebalancing_metrics.get("simulated_return", 0),
        "total_months": metrics.get("total_months", 0),
        "total_days": metrics.get("total_days", 0),
        "monthly_avg_change": metrics.get("monthly_avg_change", 0),
        "recent_monthly_avg_change": metrics.get("recent_monthly_avg_change", 0),
        "daily_rebalancing_rate": rebalancing_metrics.get("daily_rebalancing_rate", 0),
        "rebalancing_interval": rebalancing_metrics.get("rebalancing_interval"),
        "rebalancing_return_rate": rebalancing_metrics.get("rebalancing_return_rate", 0),
        "recent_return": rebalancing_metrics.get("recent_return", 0),
        "total_rebalancing_count": rebalancing_metrics.get("total_rebalancing_count", 0),
        "recent_rebalancing_count": rebalancing_metrics.get("recent_rebalancing_count", 0),
        "recent_trading_days": rebalancing_metrics.get("recent_trading_days", 0),
        "recent_max_monthly_gain": metrics.get("recent_max_monthly_gain", 0),
        "recent_max_monthly_drawdown": metrics.get("recent_max_monthly_drawdown", 0),
        "recent_max_continuous_monthly_gain": metrics.get("recent_max_continuous_monthly_gain", 0),
        "recent_max_continuous_monthly_gain_months": metrics.get(
            "recent_max_continuous_monthly_gain_months", 0
        ),
        "recent_max_continuous_monthly_drawdown": metrics.get(
            "recent_max_continuous_monthly_drawdown", 0
        ),
        "recent_max_continuous_monthly_drawdown_months": metrics.get(
            "recent_max_continuous_monthly_drawdown_months", 0
        ),
        "recent_up_months": metrics.get("recent_up_months", 0),
        "recent_down_months": metrics.get("recent_down_months", 0),
        "max_monthly_gain": metrics.get("max_monthly_gain", 0),
        "max_monthly_drawdown": metrics.get("max_monthly_drawdown", 0),
        "max_continuous_monthly_gain": metrics.get("max_continuous_monthly_gain", 0),
        "max_continuous_monthly_gain_months": metrics.get("max_continuous_monthly_gain_months", 0),
        "max_continuous_monthly_drawdown": metrics.get("max_continuous_monthly_drawdown", 0),
        "max_continuous_monthly_drawdown_months": metrics.get(
            "max_continuous_monthly_drawdown_months", 0
        ),
        "up_months": metrics.get("up_months", 0),
        "down_months": metrics.get("down_months", 0),
        "last_monthly_change": last_monthly_change,
        "last_rebalancing_date": rebalancing_metrics.get("last_rebalancing_date"),
    }
    record.update(calculate_factors(record, monthly_changes))
    return record


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
    
    # 加载调仓历史数据（分页拉取全历史，失败重试）
    history_data = load_rebalancing_history_with_retry(cube_symbol)
    if not history_data or not history_data.get('list'):
        print(f"调仓历史数据加载失败: {cube_symbol}，模拟实仓收益将退化为净值总收益")
        history_data = {}
    
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
            history_data_retry = load_rebalancing_history_with_retry(cube_symbol)
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
        simulated_return = rebalancing_metrics.get('simulated_return', 0)
        total_rebalancing_count = rebalancing_metrics.get('total_rebalancing_count', 0)
        daily_rebalancing_rate = rebalancing_metrics.get('daily_rebalancing_rate', 0)
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

        # 检查条件3：近一年日均调仓次数超过1
        if daily_rebalancing_rate > 1:
            print(f"跳过组合 {cube_symbol}: 近一年日均调仓次数超过1: {daily_rebalancing_rate:.4f}")
            return "SKIP"

        # 检查条件4：全历史模拟实盘收益为负（与总收益同口径，按换手扣费）
        if total_rebalancing_count > 0 and simulated_return < 0:
            print(f"跳过组合 {cube_symbol}: 模拟实盘收益为负: {simulated_return:.4%}")
            return "SKIP"

        # 检查条件5：总调仓次数大于交易日数
        if total_rebalancing_count > total_days:
            print(
                f"跳过组合 {cube_symbol}: 调仓次数({total_rebalancing_count})"
                f"大于交易日数({total_days})"
            )
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
    
    # 4. 写入 SQLite 并生成报表
    report_date = datetime.now().strftime('%Y%m%d')

    cube_name = "N/A"
    if data and len(data) > 0:
        cube_name = data[0].get('name', 'N/A')

    analysis_record = build_analysis_record(
        cube_symbol, cube_name, metrics, rebalancing_metrics
    )
    save_cube_analysis(
        analysis_record,
        metrics.get("monthly_data"),
        analyzed_date=report_date,
    )
    
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
        "近一年调仓次数", "","",str(rebalancing_metrics.get('recent_rebalancing_count', 0))
    ])
    report_lines.append([
        "近一年交易日数", "","",str(rebalancing_metrics.get('recent_trading_days', 0))
    ])
    report_lines.append([
        "总收益率", "","",f"{rebalancing_metrics.get('total_return', 0):.4%}"
    ])
    report_lines.append([
        "近一年收益率", "","",f"{rebalancing_metrics.get('recent_return', 0):.4%}"
    ])
    report_lines.append([
        "每次调仓收益率","","", f"{rebalancing_metrics.get('rebalancing_return_rate', 0):.6%}"
    ])

    daily_rebalancing_rate = rebalancing_metrics.get('daily_rebalancing_rate', 0)
    rebalancing_interval = rebalancing_metrics.get('rebalancing_interval')
    report_lines.append([
        "日均调仓次数", "","", f"{daily_rebalancing_rate:.6f}"
    ])
    report_lines.append([
        "调仓间隔(自然日)", "","",
        f"{rebalancing_interval:.4f}" if rebalancing_interval is not None else "N/A"
    ])
    
    # 模拟实仓收益率：全历史净值路径，按换手金额比例扣费
    simulated_return = rebalancing_metrics.get('simulated_return', 0)
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
                elif key == "近一年调仓次数":
                    result['recent_rebalancing_count'] = int(value) if value.isdigit() else 0
                elif key == "近一年交易日数":
                    result['recent_trading_days'] = int(value) if value.isdigit() else 0
                elif key == "近一年收益率":
                    result['recent_return'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "交易日数":
                    result['total_days'] = int(value) if value.isdigit() else 0
                elif key == "每次调仓收益率":
                    result['rebalancing_return_rate'] = float(value.replace('%', '')) / 100 if value else 0
                elif key == "日均调仓次数":
                    result['daily_rebalancing_rate'] = float(value) if value else 0
                elif key == "调仓间隔(自然日)":
                    if value and value != 'N/A':
                        result['rebalancing_interval'] = float(value)
                    else:
                        result['rebalancing_interval'] = None
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
        
        # 兼容旧报表：若无近一年字段则从全历史数据推算
        if 'daily_rebalancing_rate' not in result:
            recent_count = result.get('recent_rebalancing_count')
            recent_days = result.get('recent_trading_days')
            if recent_count is None:
                recent_count = result.get('total_rebalancing_count', 0)
            if recent_days is None:
                recent_days = result.get('total_days', 0)
            daily_rate, interval = calculate_daily_rebalancing_and_interval(
                recent_count, recent_days
            )
            result['daily_rebalancing_rate'] = daily_rate
            if 'rebalancing_interval' not in result:
                result['rebalancing_interval'] = interval
        
        # 如果最后调仓日期不存在或为空，尝试从API重新获取
        if not result.get('last_rebalancing_date') or result.get('last_rebalancing_date') == 'N/A':
            symbol = result.get('symbol')
            if symbol:
                try:
                    history_data = load_rebalancing_history_with_retry(symbol)
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


def _parse_monthly_df_from_csv(file_path):
    """从组合 CSV 报表解析月度统计块。"""
    rows = []
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            for line in f:
                parts = line.strip().split(",")
                if len(parts) < 14:
                    continue
                year_month = parts[0].strip()
                if not year_month or year_month == "年月":
                    continue
                if len(year_month) != 7 or year_month[4] != "-":
                    continue
                try:
                    rows.append({
                        "year_month": year_month,
                        "month_start_value": float(parts[1]),
                        "month_end_value": float(parts[2]),
                        "monthly_change": float(parts[3].replace("%", "")) / 100,
                        "avg_daily_change": float(parts[4].replace("%", "")) / 100,
                        "max_daily_change": float(parts[5].replace("%", "")) / 100,
                        "max_daily_drawdown": float(parts[6].replace("%", "")) / 100,
                        "max_continuous_gain": float(parts[7].replace("%", "")) / 100,
                        "max_continuous_drawdown": float(parts[8].replace("%", "")) / 100,
                        "max_value": float(parts[9]),
                        "min_value": float(parts[10]),
                        "amplitude": float(parts[11].replace("%", "")) / 100,
                        "up_days": int(parts[12]),
                        "down_days": int(parts[13]),
                    })
                except (TypeError, ValueError):
                    continue
    except OSError:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _load_summary_from_csv_fallback(today_date):
    """兼容旧流程：从当天 CSV 解析汇总，并回填 SQLite。"""
    report_base_dir = "report"
    today_dir = os.path.join(report_base_dir, today_date)
    if not os.path.isdir(today_dir):
        return pd.DataFrame()

    csv_files = glob.glob(os.path.join(today_dir, "*.csv"))
    if not csv_files:
        return pd.DataFrame()

    symbol_files = {}
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        match = re.match(r"^([A-Z0-9]+)_(\d{8})\.csv$", filename)
        if match:
            symbol = match.group(1)
            date_str = match.group(2)
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            if symbol not in symbol_files or date_obj > symbol_files[symbol]["date"]:
                symbol_files[symbol] = {"file_path": file_path, "date": date_obj}

    saved = 0
    for symbol, file_info in symbol_files.items():
        file_path = file_info["file_path"]
        data = parse_csv_report(file_path)
        if data:
            monthly_df = _parse_monthly_df_from_csv(file_path)
            save_cube_analysis(data, monthly_df if not monthly_df.empty else None, analyzed_date=today_date)
            saved += 1

    if saved == 0:
        return pd.DataFrame()

    print(f"从 CSV 回填 SQLite: {saved} 个组合")
    return load_summary_dataframe(today_date)


def generate_summary_report(run_date=None):
    """
    从 SQLite 生成汇总报表，并导出 Excel（兼容旧流程）。
    
    Returns:
        str: 汇总文件路径，如果失败返回None
    """
    print("开始生成汇总报表（数据来源: SQLite）...")

    today_date = run_date or datetime.now().strftime('%Y%m%d')
    df = load_summary_dataframe(today_date)

    if df.empty:
        print(f"SQLite 中未找到 {today_date} 的汇总数据，尝试从当天 CSV 回填...")
        df = _load_summary_from_csv_fallback(today_date)

    if df.empty:
        print("没有可用的汇总数据")
        return None

    print(f"汇总 {len(df)} 个组合（run_date={today_date}）")

    report_base_dir = "report"
    summary_date = today_date
    excel_filename = f"summary_{summary_date}.xlsx"

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
