# 雪球组合数据分析工具

用于分析雪球组合历史数据、计算多维度因子、管理候选列表，并同步到跟单系统。

> 使用前请配置雪球 Cookie（见下方「配置」）。  
> 有需求请提交 Issues。

## 功能特性

- 自动获取组合净值与调仓历史（调仓历史 SQLite 增量缓存）
- 月度统计、风险指标、调仓效率分析
- 模拟实仓收益（全历史净值 + 按换手金额扣费）
- 多维度因子评分与汇总排名
- 候选组合自动筛选、备份、同步跟单
- 分析结果写入 SQLite，汇总/选股从数据库生成

## 安装依赖

```bash
pip install -r requirements.txt
```

建议使用清华镜像：

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
```

## 配置

Cookie 从文件读取，**不是**写在 `config.py` 里：

```
~/agents_documents/xueqiu_cookies.txt
```

将浏览器登录雪球后的 Cookie 整行粘贴到该文件即可。`config.py` 仅保留 API 地址、`TRADE_COST` 等常量。

---

## 推荐操作流程（最重要）

### 每月例行更新（一条命令搞定）

```bash
python update_choosen.py
```

这一条命令会**自动完成**：

1. 拉取**年榜 + 月榜**组合
2. 合并 `choosen/choosen.csv` 中原有选中，去重得到候选池
3. 批量分析所有候选（写入 SQLite + 导出 CSV）
4. 从 SQLite 生成当日汇总 Excel
5. 按得分取前 6 名更新 `choosen.csv`（原列表备份为 `history_{date}.csv`）
6. （可选）同步到 `xueqiu_follower` 并生成 `position_sync` 指令——**默认关闭**

开启跟单同步：

```bash
python update_choosen.py --sync-follower
```

或在 `update_choosen.py` 顶部将 `ENABLE_FOLLOWER_SYNC = True`。

定时任务可用：

```bash
python run_monthly_update.py
```

（内部调用 `update_choosen.main()`，并推送飞书通知。）

### 还需要 `python analyst.py annual` 吗？

| 场景 | 是否需要单独跑 annual / monthly |
|------|--------------------------------|
| **每月更新候选列表** | **不需要**。`update_choosen.py` 已内含年榜+月榜拉取与分析 |
| 只想看榜单、用跳过规则过滤、**不更新 choosen** | 可以单独跑 `annual` / `monthly` |
| 月中只刷新当前跟踪组合、重新出汇总 | 跑 `python analyst.py summary` |
| 分析某一个指定组合 | `python analyst.py ZH3186221` |

**结论：正常月度维护只跑 `update_choosen.py` 即可，不必再单独跑 annual。**

---

## 命令说明

### 1. 月度选股（推荐主流程）

```bash
python update_choosen.py
# 同时更新 follower 配置并生成 position_sync：
python update_choosen.py --sync-follower
```

### 2. 刷新跟踪组合 + 汇总（不重新选股）

```bash
python analyst.py summary
```

流程：读取 `choosen/choosen.csv` → 刷新这些组合数据（SQLite）→ 从 SQLite 生成 `summary_{日期}.xlsx`。

适用于月中只想更新已跟踪组合、不出新候选列表时。

### 3. 交互式 / 单组合分析

```bash
python analyst.py                  # 交互模式
python analyst.py ZH3186221        # 单组合（不过滤跳过规则需在代码里设 apply_skip_filters=False）
python analyst.py batch ZH3186221 ZH1234567
```

### 4. 年榜 / 月榜单独分析（可选）

```bash
python analyst.py annual    # 年收益榜，应用跳过过滤
python analyst.py monthly   # 月收益榜，应用跳过过滤
```

与 `update_choosen.py` 的区别：

- `annual` / `monthly`：只分析榜单上的组合，**会跳过**表现不佳的组合，**不更新** choosen
- `update_choosen.py`：年榜 ∪ 月榜 ∪ 原选中，**不过滤跳过**（公平比分数），再选前 6

### 5. 帮助

```bash
python analyst.py --help
```

---

## 数据存储（SQLite）

分析结果持久化在 `data/` 目录（已加入 `.gitignore`）：

| 文件 / 表 | 说明 |
|-----------|------|
| `data/rebalancing_history.db` | 调仓历史缓存；无数据全量拉，有数据增量拉到重复即停 |
| `data/cube_analytics.db` → `cube_monthly` | 各组合按月统计 |
| `data/cube_analytics.db` → `cube_metrics` | 基础指标、调仓指标、因子、得分 |
| `data/cube_analytics.db` → `summary_snapshot` | 某日参与汇总的组合列表 |

数据流：

```
generate_report()  →  SQLite（主） + report/{日期}/*.csv（备查）
generate_summary_report()  →  读 SQLite  →  summary_{日期}.xlsx
update_choosen.py  →  读 SQLite 汇总  →  choosen.csv
```

若当天 SQLite 无汇总数据，`generate_summary_report` 会尝试从当天 CSV **回填**数据库。

---

## 组合代码格式

`ZH` 或 `SP` + 6～7 位数字，例如 `ZH3186221`、`SP1234567`。

---

## 指标口径摘要

### 模拟实仓收益率

- **时间范围**：全历史（与「总收益」对齐）
- **扣费**：每次调仓按 `成交额占净值比例 × 0.068%`（非全仓扣费）
- 成交额比例 ≈ `Σ|目标权重 − 原权重| / 200`

### 调仓相关（近一年）

- 日均调仓次数、调仓间隔、每次调仓收益率：基于近一年调仓记录与近一年净值

### 得分

`盈利能力因子 + 持久因子×7 + 交易效率因子×3 + 稳定因子`

### 跳过过滤（仅 `annual` / `monthly` / 默认 `generate_report`）

以下组合不生成报表（`update_choosen` 分析时**不启用**跳过）：

- 交易月数 &lt; 6
- 近一年日均调仓次数 &gt; 1
- 全历史模拟实仓收益为负
- 总调仓次数 &gt; 总交易日数
- 月均涨跌幅 &lt; 4%

---

## 输出文件

| 路径 | 说明 |
|------|------|
| `report/{日期}/{代码}_{日期}.csv` | 单组合报表（含月度明细） |
| `report/{日期}/summary_{日期}.xlsx` | 汇总 Excel（由 SQLite 生成） |
| `choosen/choosen.csv` | 当前候选 Top 6 |
| `choosen/history_{日期}.csv` | 历史备份 |

---

## 文件结构

```
xueqiu_cube_analyst/
├── analyst.py              # CLI 入口
├── update_choosen.py       # 月度选股主流程（推荐）
├── run_monthly_update.py   # 定时任务 + 飞书通知
├── data_loader.py          # 净值 / 榜单 / 调仓历史 API
├── history_store.py        # 调仓历史 SQLite
├── cube_store.py           # 分析结果 SQLite
├── data_analyst.py         # 指标计算、报表、汇总
├── sync_to_follower.py     # 同步跟单与指令
├── config.py
├── data/                   # SQLite 缓存（自动创建）
├── report/                 # CSV / Excel 输出
└── choosen/                # 候选列表
```

---

## 典型场景速查

```bash
# 场景 A：每月例行（年榜+月榜+选股+跟单）
python update_choosen.py

# 场景 B：月中只刷新已跟踪 6 个组合的数据和汇总
python analyst.py summary

# 场景 C：临时看某个组合
python analyst.py ZH3186221

# 场景 D：单独扫年榜（不更新 choosen，带过滤）
python analyst.py annual
```

---

## 注意事项

1. Cookie 过期会导致拉取失败，需更新 `~/agents_documents/xueqiu_cookies.txt`
2. 调仓历史首次全量较慢，之后走增量缓存
3. 若 `cube_metrics` 条数少于 API `totalCount`，可删除 `data/rebalancing_history.db` 后重拉
4. Excel 依赖 `openpyxl`

## 故障排除

| 现象 | 处理 |
|------|------|
| 未找到 Cookie 文件 | 创建 `~/agents_documents/xueqiu_cookies.txt` |
| 数据获取失败 / WAF 限频 | 检查网络和 Cookie；连续请求触发 WAF 时稍后再试，客户端已限速并自动重试 |
| 汇总为空 | 先跑分析或 `update_choosen.py`；或确认当天 `summary_snapshot` 有数据 |
| 调仓次数不准 / 为 0 | 检查调仓历史是否拉全；必要时删 `data/rebalancing_history.db` 重拉 |
| Excel 打不开 | `pip install openpyxl` |
