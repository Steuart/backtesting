import numpy as np
data = [1,2]
# 计算方差
sample_variance = np.var(data, ddof=1)  # 样本方差 (ddof=1)
population_variance = np.var(data)      # 总体方差 (ddof=0)

# 计算标准差
sample_stdev = np.std(data, ddof=1)    # 样本标准差
population_stdev = np.std(data)        # 总体标准差

print("样本方差:", sample_variance)
print("总体方差:", population_variance)
print("样本标准差:", sample_stdev)
print("总体标准差:", population_stdev)