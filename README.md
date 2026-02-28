# 雪球组合数据分析工具

这是一个用于分析雪球组合数据的工具，可以获取组合历史数据并进行各种指标分析，支持年榜、月榜自动分析和汇总报表生成。
> - 使用前请先在config.py中设置你的雪球网站登录后的cookie值(方法请自行google)
> - 有什么想法请提交Issues，我来帮你实现

## 功能特性

- 自动获取雪球组合历史数据
- 计算每日涨跌比例
- 生成月度分析报表
- 计算回撤、波动率等风险指标
- 支持交互式和批量分析模式
- **年收益榜单自动分析**
- **月收益榜单自动分析**
- **智能组合过滤**（自动跳过表现不佳的组合）
- **汇总报表生成**（Excel格式，包含多维度因子分析）

## 安装依赖

```bash
pip install -r requirements.txt
```

建议使用清华镜像源：
```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
```

## 使用方法

### 1. 交互模式（推荐）
```bash
python analyst.py
```
程序会提示输入组合代码，支持连续分析多个组合。

### 2. 单次分析
```bash
python analyst.py ZH3186221
```
由于对素质差的组合进行了自动跳过，所以不进行跳过生成数据时，请需添加apply_skip_filters=False参数
```bash
python analyst.py ZH3186221 apply_skip_filters=False
```

### 3. 批量分析
```bash
python analyst.py batch ZH3186221 ZH1234567 ZH7890123
```

### 4. 年收益榜单分析
```bash
python analyst.py annual
```
自动获取年收益榜单，分析所有上榜组合并生成报表。

### 5. 月收益榜单分析
```bash
python analyst.py monthly
```
自动获取月收益榜单，分析所有上榜组合并生成报表。

### 6. 汇总报表生成
```bash
python analyst.py summary
```
对 `report/` 目录下的所有报表进行汇总，生成Excel格式的综合分析报表。支持新的日期子目录结构。

### 7. 查看帮助
```bash
python analyst.py --help
```

## 组合代码格式

雪球组合代码格式为：`ZH` 或 `SP` + 6-7位数字，例如：`ZH3186221` 或 `SP1234567`
- `ZH` 开头：模拟组合
- `SP` 开头：实盘组合

## 输出报表

### 1. 单个组合报表（CSV格式）
分析完成后，会在 `report/{日期}/` 目录下生成CSV格式的报表文件，文件名格式为：`{组合代码}_{日期}.csv`

例如：`report/20251020/ZH3186221_20251020.csv`

#### 报表内容包括：

1. **主要指标**
   - 月均涨跌比例
   - 近一年月均涨跌比例
   - 月最大回撤
   - 日最大回撤
   - 日回撤累积
   - 日均波动幅度

2. **月度详细数据**
   - 每月涨跌比例
   - 月度波动率
   - 月初/月末值等

### 2. 汇总报表（Excel格式）
使用 `python analyst.py summary` 命令生成，文件名格式为：`summary_{日期}.xlsx`

#### 汇总报表包含以下字段：

**基础信息**
- 组合链接、组合名称、总收益、模拟实仓收益率、交易月数

**收益指标**
- 月均涨幅、近年月均涨幅、日均调仓次数、每次调仓收益率

**风险指标**
- 近年最大月涨幅、近年最大月回撤、最大月涨幅、最大月回撤

**连续性指标**
- 近年最大连续涨幅、近年最大连续上涨月数、近年最大连续跌幅、近年最大连续下跌月数
- 最大连续涨幅、最大连续上涨月数、最大连续跌幅、最大连续下跌月数

**统计指标**
- 近年上涨月数、近年下跌月数、上涨月数、下跌月数

**多维度因子**
- **盈利能力因子**：月均涨幅×8 + 近年月均涨幅×12
- **稳定因子**：基于月涨幅曲线平滑度的加权评估
- **交易效率因子**：基于次均收益的S型函数评估
- **持久因子**：基于交易持续时间的对数标准化
- **得分**：盈利能力因子 + 持久因子×7 + 交易效率因子×3 + 稳定因子

### 3. 智能过滤机制
在年榜和月榜分析中，程序会自动跳过以下类型的组合：
- 模拟实盘收益为负的组合
- 总调仓次数大于交易日数的组合
- 月均涨跌幅小于4%的组合

## 文件结构

```
cube_analyst/
├── config.py          # 配置文件（包含URL模板和Cookie）
├── data_loader.py     # 数据加载模块
├── data_analyst.py    # 数据分析模块
├── analyst.py         # 主入口文件
├── requirements.txt   # 依赖包列表
├── README.md         # 说明文档
└── report/           # 报表输出目录（自动创建）
```

## 使用示例

### 快速开始
```bash
# 1. 分析单个组合
python analyst.py ZH3186221

# 2. 分析年收益榜单
python analyst.py annual

# 3. 分析月收益榜单
python analyst.py monthly

# 4. 生成汇总报表
python analyst.py summary
```

### 典型工作流程
```bash
# 第一步：分析年榜和月榜
python analyst.py annual
python analyst.py monthly

# 第二步：生成汇总报表
python analyst.py summary

# 第三步：查看生成的Excel文件
# 文件位置：report/{日期}/summary_YYYYMMDD.xlsx
```

## 注意事项

1. 使用前请确保 `config.py` 中的Cookie配置有效
2. 网络连接需要能够访问雪球网站
3. 报表文件使用UTF-8编码，可用Excel打开
4. 建议在分析前检查组合代码是否正确
5. 汇总报表会自动选择每个组合的最新报表文件
6. 年榜和月榜分析会自动过滤表现不佳的组合

## 故障排除

- **数据获取失败**：检查网络连接和Cookie配置
- **组合代码无效**：确认代码格式为ZH或SP+6-7位数字
- **报表生成失败**：检查磁盘空间和写入权限
- **Excel文件无法打开**：确保安装了openpyxl库
- **汇总报表为空**：检查report目录下是否有CSV报表文件（支持新的日期子目录结构）
