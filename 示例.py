import numpy as np
from scipy.stats import weibull_min
import matplotlib.pyplot as plt

# 示例风速数据
wind_speeds = np.array([2.5, 3.2, 4.1, 1.8, 5.6, 3.9, 4.7, 2.9, 3.5, 4.3])

# 威布尔分布拟合
shape, loc, scale = weibull_min.fit(wind_speeds, floc=0)

# 定义风速区间
wind_speed_bins = np.arange(0, int(np.max(wind_speeds)) + 2)

# 计算每个区间的概率
probabilities = []
for i in range(len(wind_speed_bins) - 1):
    lower_bound = wind_speed_bins[i]
    upper_bound = wind_speed_bins[i + 1]
    prob = weibull_min.cdf(upper_bound, c=shape, loc=loc, scale=scale) - weibull_min.cdf(lower_bound, c=shape, loc=loc, scale=scale)
    probabilities.append(prob)

# 打印结果
for i in range(len(wind_speed_bins) - 1):
    print(f"风速区间 [{wind_speed_bins[i]}, {wind_speed_bins[i + 1]}): 概率 = {probabilities[i]:.4f}")

# 可视化结果
plt.bar(wind_speed_bins[:-1], probabilities, width=1, edgecolor='k')
plt.xlabel('风速 (m/s)')
plt.ylabel('概率')
plt.title('威布尔分布拟合风速数据的区间概率')
plt.xticks(wind_speed_bins[:-1])
plt.show()