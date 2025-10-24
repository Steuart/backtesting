# Backtrader 演示程序

这是一个使用 Backtrader 框架的量化交易回测演示程序。

## 功能特性

- **简单移动平均线交叉策略**: 实现了基于短期和长期移动平均线交叉的交易策略
- **完整的回测框架**: 包含订单管理、交易通知、性能分析等功能
- **示例数据生成**: 自动生成模拟的股价数据用于回测
- **性能分析**: 计算夏普比率、总收益率、最大回撤等关键指标
- **图表可视化**: 生成K线图和交易信号图表

## 安装依赖

首先激活虚拟环境，然后安装所需的依赖包：

```bash
# 激活虚拟环境
source .venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

## 运行程序

```bash
python main.py
```

## 策略说明

### SimpleMovingAverageStrategy

这是一个基于移动平均线交叉的简单策略：

- **买入信号**: 当10日移动平均线上穿30日移动平均线时买入
- **卖出信号**: 当10日移动平均线下穿30日移动平均线时卖出

### 参数配置

- `short_period`: 短期移动平均线周期（默认10）
- `long_period`: 长期移动平均线周期（默认30）
- `printlog`: 是否打印交易日志（默认True）

## 回测设置

- **初始资金**: 10,000元
- **手续费**: 0.1%
- **数据周期**: 2023年全年日线数据
- **交易模式**: 全仓交易

## 输出结果

程序运行后会显示：

1. 详细的交易日志（买入/卖出信号和执行情况）
2. 最终资金和收益情况
3. 关键性能指标：
   - 夏普比率
   - 总收益率
   - 最大回撤
4. 可视化图表（如果matplotlib可用）

## 扩展建议

1. **添加更多技术指标**: RSI、MACD、布林带等
2. **优化参数**: 使用网格搜索或遗传算法优化策略参数
3. **风险管理**: 添加止损、止盈、仓位管理等功能
4. **实盘数据**: 集成真实的股票或加密货币数据源
5. **多策略组合**: 实现多个策略的组合和轮换

## 注意事项

- 这只是一个演示程序，实际交易前请充分测试和验证策略
- 历史回测结果不代表未来表现
- 请根据自己的风险承受能力调整策略参数

---

## 接入 TimescaleDB 基金数据（fund_market）

项目提供 `datafeed/TimescaleFundData`，使用 psycopg2 直接读取 TimescaleDB 的 `trader` 库中 `fund_market` 表，并适配为 Backtrader 的数据源。

### 连接与使用示例

```python
import backtrader as bt
from datafeed import TimescaleFundData
from main import SimpleMovingAverageStrategy  # 或者你自己的策略

# 创建引擎
cerebro = bt.Cerebro()

# 加载基金数据（示例基金代码与时间范围）
data = TimescaleFundData(
    conn_str='postgresql://user:pass@host:5432/trader',
    fund_code='110022',
    start='2023-01-01',
    end='2023-12-31'
)

# 添加数据与策略
cerebro.adddata(data)
cerebro.addstrategy(SimpleMovingAverageStrategy)

# 基本回测设置
cerebro.broker.setcash(10000.0)
cerebro.broker.setcommission(commission=0.001)

# 运行
cerebro.run()
```

### 列名映射与自动推断

- 默认会自动从 `fund_market` 表中推断以下关键列（若存在）：
  - 时间列：`time` / `timestamp` / `ts` / `date` / `dt`
  - 收盘价/净值：`close` / `price` / `nav` / `last`
  - 代码列：`fund_code` / `code` / `symbol`
  - 其他可选：`open`、`high`、`low`、`volume`
- 若你的表结构不同，可显式指定列名，例如：

```python
# 显式指定时间与价格列名（例如时间列为 ts，净值列为 nav）
data = TimescaleFundData(
    conn_str='postgresql://user:pass@host:5432/trader',
    fund_code='110022',
    start='2023-01-01',
    end='2023-12-31',
    time_col='ts',
    close_col='nav'
)
```

### 数据缺失处理

- 若仅有单价（如净值），会自动将 `Open/High/Low` 填充为 `Close`，`Volume` 填充为 0，以满足 Backtrader 的 OHLCV 结构要求。