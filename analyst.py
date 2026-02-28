#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
雪球组合分析工具
主入口文件，接收用户输入的cube_symbol并生成分析报表
"""

from data_analyst import generate_report, generate_summary_report
from data_loader import load_annual_rank_data, load_monthly_rank_data
import sys
import re


def validate_cube_symbol(symbol):
    """
    验证cube_symbol格式
    
    Args:
        symbol (str): 组合代码
        
    Returns:
        bool: 是否有效
    """
    # 雪球组合代码格式：ZH或SP + 6或7位数字
    pattern = r'^(ZH|SP)\d{6,7}$'
    return bool(re.match(pattern, symbol))


def main():
    """
    主函数：接收用户输入并执行分析
    """
    print("=" * 60)
    print("雪球组合数据分析工具")
    print("=" * 60)
    print()
    
    # 获取用户输入
    while True:
        try:
            cube_symbol = input("请输入组合代码 (格式: ZH3186221): ").strip().upper()
            
            if not cube_symbol:
                print("请输入有效的组合代码")
                continue
                
            if not validate_cube_symbol(cube_symbol):
                print("组合代码格式不正确，应为ZH或SP开头加6-7位数字，如：ZH3186221或SP1234567")
                continue
            
            print(f"\n开始分析组合: {cube_symbol}")
            print("-" * 40)
            
            # 执行分析（指定组合分析：不受收益率等限制，不跳过）
            result = generate_report(cube_symbol, apply_skip_filters=False)
            
            if result:
                print("\n" + "=" * 60)
                print("分析完成！")
                print(f"报表文件: {result}")
                print("=" * 60)
            else:
                print("\n分析失败，请检查网络连接或组合代码是否正确")
            
            # 询问是否继续
            while True:
                choice = input("\n是否继续分析其他组合？(y/n): ").strip().lower()
                if choice in ['y', 'yes', '是']:
                    print()
                    break
                elif choice in ['n', 'no', '否']:
                    print("感谢使用！")
                    return
                else:
                    print("请输入 y 或 n")
                    
        except KeyboardInterrupt:
            print("\n\n程序被用户中断")
            sys.exit(0)
        except Exception as e:
            print(f"\n发生错误: {e}")
            print("请重试")


def batch_analysis(symbols):
    """
    批量分析多个组合
    
    Args:
        symbols (list): 组合代码列表
    """
    print("=" * 60)
    print("雪球组合批量分析工具")
    print("=" * 60)
    
    results = []
    total = len(symbols)
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\n[{i}/{total}] 分析组合: {symbol}")
        print("-" * 40)
        
        result = generate_report(symbol, apply_skip_filters=False)
        results.append({
            'symbol': symbol,
            'success': result is not None,
            'report_path': result
        })
    
    # 输出批量分析结果
    print("\n" + "=" * 60)
    print("批量分析结果")
    print("=" * 60)
    
    success_count = 0
    for result in results:
        status = "成功" if result['success'] else "失败"
        print(f"{result['symbol']}: {status}")
        if result['success']:
            print(f"  报表: {result['report_path']}")
            success_count += 1
    
    print(f"\n总计: {success_count}/{total} 个组合分析成功")


def process_rank_data(rank_data, rank_type):
    """
    处理榜单数据，生成组合报表
    
    Args:
        rank_data (dict): 榜单数据
        rank_type (str): 榜单类型 ('annual' 或 'monthly')
    """
    if not rank_data or 'list' not in rank_data:
        print(f"获取{rank_type}榜单数据失败")
        return
    
    cube_list = rank_data['list']
    total_cubes = len(cube_list)
    
    print(f"开始处理{rank_type}榜单，共 {total_cubes} 个组合")
    print("=" * 60)
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for i, cube_info in enumerate(cube_list, 1):
        symbol = cube_info.get('symbol', '')
        name = cube_info.get('name', '')
        
        print(f"\n[{i}/{total_cubes}] 处理组合: {symbol} ({name})")
        print("-" * 40)
        
        # 直接生成报表，在generate_report内部进行过滤判断
        result = generate_report(symbol)
        if result == "SKIP":
            skip_count += 1
        elif result:
            print(f"成功生成报表: {result}")
            success_count += 1
        else:
            print(f"生成报表失败")
            error_count += 1
    
    # 输出统计结果
    print("\n" + "=" * 60)
    print(f"{rank_type}榜单处理结果")
    print("=" * 60)
    print(f"总组合数: {total_cubes}")
    print(f"成功生成报表: {success_count}")
    print(f"跳过组合: {skip_count}")
    print(f"失败组合: {error_count}")
    print("=" * 60)


def annual_rank_analysis():
    """
    年收益榜单分析
    """
    print("=" * 60)
    print("年收益榜单分析")
    print("=" * 60)
    
    # 获取年榜数据
    rank_data = load_annual_rank_data()
    if not rank_data:
        print("获取年榜数据失败")
        return
    
    # 处理年榜数据
    process_rank_data(rank_data, "年收益")


def monthly_rank_analysis():
    """
    月收益榜单分析
    """
    print("=" * 60)
    print("月收益榜单分析")
    print("=" * 60)
    
    # 获取月榜数据
    rank_data = load_monthly_rank_data()
    if not rank_data:
        print("获取月榜数据失败")
        return
    
    # 处理月榜数据
    process_rank_data(rank_data, "月收益")


def summary_analysis():
    """
    汇总报表分析
    """
    print("=" * 60)
    print("汇总报表分析")
    print("=" * 60)
    
    # 生成汇总报表
    result = generate_summary_report()
    if result:
        print(f"汇总报表生成成功: {result}")
    else:
        print("汇总报表生成失败")


if __name__ == "__main__":
    # 检查命令行参数
    if len(sys.argv) > 1:
        if sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print("""
雪球组合分析工具使用说明：

1. 交互模式（默认）:
   python analyst.py

2. 批量分析模式:
   python analyst.py batch ZH3186221 ZH1234567 ZH7890123

3. 单次分析模式:
   python analyst.py ZH3186221

4. 年收益榜单分析:
   python analyst.py annual

5. 月收益榜单分析:
   python analyst.py monthly

6. 汇总报表分析:
   python analyst.py summary

7. 帮助信息:
   python analyst.py --help
            """)
            sys.exit(0)
        elif sys.argv[1] == 'annual':
            # 年收益榜单分析
            annual_rank_analysis()
        elif sys.argv[1] == 'monthly':
            # 月收益榜单分析
            monthly_rank_analysis()
        elif sys.argv[1] == 'summary':
            # 汇总报表分析
            summary_analysis()
        elif sys.argv[1] == 'batch' and len(sys.argv) > 2:
            # 批量分析模式
            symbols = sys.argv[2:]
            batch_analysis(symbols)
        else:
            # 单次分析模式
            symbol = sys.argv[1].upper()
            if validate_cube_symbol(symbol):
                print(f"分析组合: {symbol}")
                result = generate_report(symbol, apply_skip_filters=False)
                if result:
                    print(f"分析完成，报表: {result}")
                else:
                    print("分析失败")
            else:
                print("组合代码格式不正确")
                sys.exit(1)
    else:
        # 交互模式
        main()
