import concurrent.futures
import pandas as pd
import os
import struct
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import rayleigh, weibull_min

MAX_THREAD = 8


def weibull(ws_data, wt_type=None):
    """
    威布尔分布计算不同风速下发生的概率
    :param wt_type:
    :param ws_data:
    :return:
    """

    shape, loc, scale = weibull_min.fit(ws_data, floc=0)
    # 定义风速区间
    # wind_speed_bins = np.arange(0, int(np.max(ws_data)) + 2)
    wind_speed_bins = np.arange(-0.25, 50, 0.5)
    # 计算每个区间的概率
    probabilities = {}
    for i in range(len(wind_speed_bins) - 1):
        lower_bound = wind_speed_bins[i]
        upper_bound = wind_speed_bins[i + 1]
        prob = weibull_min.cdf(upper_bound, c=shape, loc=loc, scale=scale) - weibull_min.cdf(lower_bound, c=shape,
                                                                                             loc=loc, scale=scale)
        temp = (upper_bound + lower_bound) / 2
        probabilities[round(temp, 1)] = prob

    return probabilities, wt_type


def get_ws_frequency(wt_data):
    """
    获取不同机型的功率曲线
    :param wt_data: 所有风机的数据
    :return:
    """
    result_probabilities = {}
    if not isinstance(wt_data, pd.DataFrame):
        wt_data = pd.DataFrame(wt_data)
    wt_group_data = wt_data.groupby('wt_type')

    with concurrent.futures.ProcessPoolExecutor(max_workers= 8) as executor:
        to_do = []
        for wt_type, data in wt_group_data:
            future = executor.submit(weibull, data['ws'], wt_type)
            to_do.append(future)
        for future in concurrent.futures.as_completed(to_do):
            result = future.result()
            result_probabilities[result[1]] = result[0]

    # for wt_type, data in wt_group_data:
    #     probabilities, wt_type = weibull(data['ws'], wt_type)
    #     result_probabilities[wt_type] = probabilities
    return result_probabilities


if __name__ == '__main__':
    wind_speeds = np.array([2.5, 3.2, 4.1, 1.8, 5.6, 3.9, 4.7, 2.9, 3.5, 4.3])
    result = weibull(wind_speeds)
    print(result)
    plt.bar(result.keys(), result.values(), width=1, edgecolor='k')
    plt.xlabel('风速 (m/s)')
    plt.ylabel('概率')
    plt.title('威布尔分布拟合风速数据的区间概率')
    plt.show()
