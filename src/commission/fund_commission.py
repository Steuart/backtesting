from backtrader import CommissionInfo

class FundCommission(CommissionInfo):
    params = (
        ('commission', 0.00005),  # 默认费率0.005%
        ('min_commission', 0.1),  # 最低佣金
    )

    def _getcommission(self, size, price, pseudoexec):
        commission = abs(size) * price * self.p.commission  # 计算原始佣金
        return max(commission, self.p.min_commission)  # 应用最低限制
