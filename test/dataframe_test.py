import pandas as pd
import numpy as np
from sqlalchemy import text
from database import db_pool
def date():
    print(pd.Timestamp.now())

def main():
    engine = db_pool.get_engine()
    # 直接使用近一年行情数据，不查询 fund 表代码
    # 2) 查询近一年 fund_market 的行情（仅取 close）
    end_date = pd.Timestamp.today().normalize()
    start_date = end_date - pd.DateOffset(years=1)

    market_df = pd.read_sql(
        text(
            """
            SELECT time, symbol, close
            FROM fund_market
            WHERE time >= :start_date AND time <= :end_date
            ORDER BY time ASC
            """
        ),
        engine,
        params={"start_date": start_date, "end_date": end_date},
    )
    # 不过滤 symbol，保留所有标的

    # 3) 以时间为索引，按 symbol 透视为列，值为 close
    market_df['time'] = pd.to_datetime(market_df['time'])
    pivot_close = market_df.pivot(index='time', columns='symbol', values='close')

    # 去掉完全为空的行，避免相关性计算时全 NaN 行干扰
    pivot_close = pivot_close.dropna(how='all')

    # 4) 计算基金之间的收益率相关性，并过滤最小重叠样本数
    returns = pivot_close.pct_change()
    returns = returns.dropna(how='all')
    corr_matrix = returns.corr(method='pearson', min_periods=60)
    # 使用不易冲突的轴名称，避免 reset_index 重名问题
    corr_matrix = corr_matrix.rename_axis(index='fund_i', columns='fund_j')

    # 有效重叠样本数矩阵（用于过滤虚高相关性）
    valid = returns.notna().astype(int)
    overlap_counts = valid.T.dot(valid)
    overlap_counts = overlap_counts.rename_axis(index='fund_i', columns='fund_j')

    # 输出相关性矩阵前几行，便于查看
    print('基金收益率相关性矩阵（部分预览）:')
    print(corr_matrix.head())

    # 阈值设定：相关性 & 最小重叠样本数
    threshold = 0.8
    min_overlap = 60

    # 5) 列出满足阈值的基金对（仅取上三角，去除自相关）
    upper_mask = np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
    corr_pairs = (
        corr_matrix.where(upper_mask)
        .stack()
        .reset_index(name='corr')
    )
    count_pairs = (
        overlap_counts.where(upper_mask)
        .stack()
        .reset_index(name='n')
    )
    # 对齐新的轴名称
    pairs_df = corr_pairs.merge(count_pairs, on=['fund_i', 'fund_j'])
    strong_pairs = pairs_df[
        (pairs_df['corr'] > threshold) & (pairs_df['n'] >= min_overlap)
    ].sort_values('corr', ascending=False)

    if strong_pairs.empty:
        print(f'无相关性大于 {threshold} 且重叠样本 >= {min_overlap} 的基金对')
    else:
        print(f'\n相关性 > {threshold} 且重叠样本 >= {min_overlap} 的基金对（按相关性降序）：')
        for _, row in strong_pairs.iterrows():
            print(f"{row['fund_i']} - {row['fund_j']}: corr={row['corr']:.3f}, n={int(row['n'])}")

    # 6) 根据强相关关系进行分组（连通分量），把互相关的基金放在同一组
    threshold = 0.8
    symbols = corr_matrix.columns.tolist()
    adjacency = {s: set() for s in symbols}

    for _, row in strong_pairs.iterrows():
        a = row['fund_i']
        b = row['fund_j']
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)

    visited = set()
    groups = []
    for s in symbols:
        if s in visited:
            continue
        # 仅分组有边的节点（与至少一个基金强相关）
        if len(adjacency.get(s, set())) == 0:
            continue
        stack = [s]
        comp = []
        visited.add(s)
        while stack:
            u = stack.pop()
            comp.append(u)
            for v in adjacency.get(u, set()):
                if v not in visited:
                    visited.add(v)
                    stack.append(v)
        groups.append(sorted(comp))

    if not groups:
        print('\n无强相关基金组（阈值 > 0.8）')
    else:
        print(f"\n按相关性分组（阈值 > {threshold}）：")
        for i, comp in enumerate(groups, start=1):
            print(f"组 {i}（{len(comp)}）: {', '.join(comp)}")


if __name__ == '__main__':
    date()
