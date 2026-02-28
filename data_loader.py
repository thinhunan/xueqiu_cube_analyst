import requests
from config import CUBE_DATA_URL, HISTORY_DATA_URL, COOKIE, ANNUAL_RANK_URL, MONTHLY_RANK_URL


def load_cube_data(cube_symbol):
    """
    根据cube_symbol请求雪球组合数据
    
    Args:
        cube_symbol (str): 组合代码，如ZH3186221
        
    Returns:
        list: 返回的数据列表，如果请求失败返回None
    """
    try:
        # 使用配置中的URL模板拼出完整的请求URL
        url = CUBE_DATA_URL.replace("<cube_symbol>", cube_symbol)
        
        # 设置请求头，包含cookie
        headers = {
            'Cookie': COOKIE,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 发送GET请求
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # 如果状态码不是200会抛出异常
        
        # 解析JSON数据
        data = response.json()
        
        print(f"成功获取组合 {cube_symbol} 的数据")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取数据时发生未知错误: {e}")
        return None


if __name__ == "__main__":
    # 测试功能
    test_symbol = "ZH3186221"
    result = load_cube_data(test_symbol)
    if result:
        print(f"获取到 {len(result)} 条数据")
        if result and len(result) > 0:
            print(f"第一条数据示例: {result[0]}")


def load_rebalancing_history(cube_symbol):
    """
    根据cube_symbol请求雪球组合调仓历史数据
    
    Args:
        cube_symbol (str): 组合代码，如ZH3186221
        
    Returns:
        dict: 返回的调仓历史数据，如果请求失败返回None
    """
    try:
        # 使用配置中的URL模板拼出完整的请求URL
        url = HISTORY_DATA_URL.replace("<cube_symbol>", cube_symbol)
        
        # 设置请求头，包含cookie
        headers = {
            'Cookie': COOKIE,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 发送GET请求
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # 如果状态码不是200会抛出异常
        
        # 解析JSON数据
        data = response.json()
        
        print(f"成功获取组合 {cube_symbol} 的调仓历史数据")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求调仓历史数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析调仓历史JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取调仓历史数据时发生未知错误: {e}")
        return None


def load_annual_rank_data():
    """
    获取年收益榜单数据
    
    Returns:
        dict: 返回的年榜数据，如果请求失败返回None
    """
    try:
        # 设置请求头，包含cookie
        headers = {
            'Cookie': COOKIE,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 发送GET请求
        response = requests.get(ANNUAL_RANK_URL, headers=headers, timeout=30)
        response.raise_for_status()  # 如果状态码不是200会抛出异常
        
        # 解析JSON数据
        data = response.json()
        
        print(f"成功获取年收益榜单数据，共 {data.get('count', 0)} 个组合")
        return data
        
    except requests.exceptions.RequestException as e:
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
        # 设置请求头，包含cookie
        headers = {
            'Cookie': COOKIE,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 发送GET请求
        response = requests.get(MONTHLY_RANK_URL, headers=headers, timeout=30)
        response.raise_for_status()  # 如果状态码不是200会抛出异常
        
        # 解析JSON数据
        data = response.json()
        
        print(f"成功获取月收益榜单数据，共 {data.get('count', 0)} 个组合")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"请求月榜数据失败: {e}")
        return None
    except ValueError as e:
        print(f"解析月榜JSON数据失败: {e}")
        return None
    except Exception as e:
        print(f"获取月榜数据时发生未知错误: {e}")
        return None
