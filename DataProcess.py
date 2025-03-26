import concurrent.futures
import pandas as pd
import numpy as np
from scipy.interpolate import UnivariateSpline
from WeibullModel import get_ws_frequency, weibull
import matplotlib.pyplot as plt

MAX_PROCESS = 8


def separate_bin(d, label, bins):
    """
    数据分仓（bin方法）标记
    :param bins: bin列表
    :param d:dataframe 格式数据
    :param label: 分仓依据，以风速或者转速为依据。
    :return:
    """
    data = d.copy(deep=True)
    labels = [round(i, 1) for i in (bins[:-1] + bins[1:]) / 2]
    filed_label = '%s_label' % label
    data[filed_label] = pd.cut(data[label], bins=bins, labels=labels)
    return data, filed_label


def power_curve_fit_plus(data, wt_type=None):
    """
    :param wt_type:
    :param data: 机组10min统计数据,该数据必须为过滤后的干净数据
    :return:
    """
    data = data.copy(deep=True)
    # 这些字段是生成机组功率曲线的关键字段,不管传入数据是否有以下这些字段
    required_statistic_field = {'ws', 'power'}
    label_data, label = separate_bin(data, 'ws', np.arange(-0.25, 50, 0.5))
    # label_data, label = separate_bin(data, 'ws', np.arange(0, 50, 1))
    real_statistic_field = set(label_data.columns) & required_statistic_field
    agg_fields = {i: 'mean' for i in real_statistic_field}
    agg_fields.update({label: 'count'})
    pc_data = label_data.groupby(label, observed=True).agg(agg_fields)
    pc_data.rename(columns={label: 'bin_points_count'}, inplace=True)
    pc_data.reset_index(inplace=True)
    # 删除功率曲线中bin的数据点数少于0的bin。
    # pc_data.drop(np.where(pc_data['bin_points_count'] < 3)[0], inplace=True)
    # pc_data = pc_data[pc_data['bin_points_count'] >= 3]
    pc_data = pc_data[pc_data['bin_points_count'] >= 10]

    return pc_data, wt_type


def power_curve_fit(curve_data, wt_type=None):
    """
    机组功率曲线拟合
    :param wt_type:
    :param curve_data:功率曲线数据，包含风速ws，功率power;
    :return:
    """
    curve_data.sort_values(by='ws', inplace=True)
    curve = UnivariateSpline(curve_data['ws'].to_list(), curve_data['power'].to_list(), k=1, ext=0)

    # fig = plt.figure(figsize=(6, 4), dpi=150)
    # plt.title('功率曲线满发段功率')
    # plt.plot(curve_data['ws'], curve(curve_data['ws']), c='r', label='功率曲线')
    # plt.scatter(curve_data['ws'], curve(curve_data['ws']), c='blue')
    # plt.show()
    return curve, wt_type


def get_wt_power_curve(wt_data):
    """
    获取不同机型的功率曲线
    :param wt_data: 所有风机的数据
    :return:
    """
    result_curve = {}
    if not isinstance(wt_data, pd.DataFrame):
        wt_data = pd.DataFrame(wt_data)
    wt_group_data = wt_data.groupby('wt_type')

    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PROCESS) as executor:
        to_do = []
        for wt_type, data in wt_group_data:
            future = executor.submit(power_curve_fit_plus, data, wt_type)
            to_do.append(future)
        for future in concurrent.futures.as_completed(to_do):
            result = future.result()
            result_curve[result[1]] = result[0]

    # for wt_type, data in wt_group_data:
    #     curve, wt_type = power_curve_fit_plus(data, wt_type)
    #     result_curve[wt_type] = curve
    return result_curve


def power_calculation(wt_data, wf_data):
    # 风机电力测算
    curve_data = get_wt_power_curve(wt_data)
    frequency_data = get_ws_frequency(wt_data)
    # print(frequency_data)
    wt_power_dict = {}
    for wt_type, frequency in frequency_data.items():
        power = 0
        wt_curve_data = curve_data[wt_type]
        for ws, p in frequency.items():
            power = power + wt_curve_data(ws) * p * 365 * 24
        wt_power_dict[wt_type] = power

    # 风电场平均电量测算
    wf_curve_data, _ = power_curve_fit(wf_data)
    wf_frequency_data, _ = weibull(wf_data['ws'])
    # print(wf_frequency_data)
    wf_power = 0
    for ws, p in wf_frequency_data.items():
        wf_power = wf_power + wf_curve_data(ws) * p * 365 * 24
    # print(wt_power_dict, wf_power)
    return wt_power_dict, wf_power


def power_calculation_plus(wt_data, wf_data):
    # 风机电力测算
    curve_data = get_wt_power_curve(wt_data)
    frequency_data = get_ws_frequency(wt_data)
    print(curve_data, frequency_data)
    wt_power_dict = {}
    for wt_type, frequency in frequency_data.items():
        power = 0
        wt_curve_data = curve_data[wt_type]
        for ws, p in frequency.items():
            # power = power + curve_data[wt_type](ws) * p * 365 * 24
            pre = wt_curve_data.loc[wt_curve_data['ws_label'] == ws]['power']
            if not pre.empty:
                power = power + pre.iloc[0] * p * 365 * 24

        wt_power_dict[wt_type] = round(power/10000, 2)

    # 风电场平均电量测算
    wf_curve_data, _ = power_curve_fit_plus(wf_data)
    wf_frequency_data, _ = weibull(wf_data['ws'])
    # print(wf_curve_data, wf_frequency_data)
    wf_power = 0
    for ws, p in wf_frequency_data.items():
        pre = wf_curve_data.loc[wf_curve_data['ws_label'] == ws]['power']
        if not pre.empty:
            wf_power = wf_power + pre.iloc[0] * p * 365 * 24
    wf_power = round(wf_power/10000, 2)
    # print(wt_power_dict, wf_power)
    return wt_power_dict, wf_power


if __name__ == '__main__':
    # 设置随机种子以确保结果可重复
    np.random.seed(0)

    # 生成ws列，范围在2到20之间
    ws = np.random.uniform(2, 20, 100)

    # 生成power列，这里假设power与ws的平方成正比
    power = ws ** 2

    # 生成wt_type列，'GW1000'和'GW2000'各50个
    wt_type = ['GW1000'] * 50 + ['GW2000'] * 50

    # 创建DataFrame
    df = pd.DataFrame({
        'ws': ws,
        'power': power,
        'wt_type': wt_type
    })
    # 打乱行顺序以确保wt_type的分布是随机的
    df = df.sample(frac=1).reset_index(drop=True)
    pc_data, _ = power_curve_fit_plus(df)
    frequency_data = get_ws_frequency(df)

    fig = plt.figure(figsize=(6, 4), dpi=150)
    plt.title('功率曲线满发段功率')
    plt.plot(pc_data['ws'], pc_data['power'], c='b', label='功率曲线')
    plt.plot(df['ws'], pc_data1(df['ws']), c='r', label='功率曲线')
    plt.show()
