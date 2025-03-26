import math

import pandas as pd
import numpy as np
from DataProcess import power_calculation_plus, power_calculation
from Hbase import hbase_data_get
from pyecharts.charts import Bar, Line
import pyecharts.options as opt
import matplotlib
import streamlit as st
import matplotlib.pyplot as plt

matplotlib.rcParams['font.sans-serif'] = ['SimHei']

PowerCurve = {3.0: 15.41, 3.5: 68.54, 4.0: 121.67, 4.5: 183.56, 5.0: 244.26, 5.5: 356.5,
               6.0: 446.2, 6.5: 580.93, 7.0: 706.97, 7.5: 872.96, 8.0: 1049.23, 8.5: 1226.03,
               9.0: 1393.31, 9.5: 1541.67, 10.0: 1693.93, 10.5: 1849.58, 11.0: 1944.77,
               11.5: 1967.33, 12.0: 2000.0, 12.5: 2000.0, 13.0: 2000.0, 13.5: 2000.0,
               14.0: 2000.0, 14.5: 2000.0, 15.0: 2000.0, 15.5: 2000.0, 16.0: 2000.0,
               16.5: 2000.0, 17.0: 2000.0, 17.5: 2000.0, 18.0: 2000.0, 18.5: 2000.0,
               19.0: 2000.0, 19.5: 2000.0, 20.0: 2000.0, 20.5: 2000.0, 21.0: 2000.0,
               21.5: 2000.0, 22.0: 2000.0, 22.5: 2000.0, 23.0: 2000.0, 23.5: 2000.0,
               24.0: 2000.0, 24.5: 2000.0, 25.0: 2010.0}


def k_visualization(k_dict, k_standard=0.95):
    x = list(k_dict.keys())
    y = list(k_dict.values())
    fig, ax = plt.subplots()
    # 绘图
    plt.plot(x, y, label='机组饱和度K值', color='green', marker='.')
    plt.axhline(y=k_standard, color='red', linestyle='--', label='k值标准95%')
    # 在柱状图上显示具体数值, ha参数控制水平对齐方式, va控制垂直对齐方式
    for x1, yy in zip(x, y):
        plt.text(x1, yy, str(yy), ha='center', va='bottom', fontsize=5, rotation=0)
    # 设置标题
    plt.title("机组饱和度K值图示")
    # 为两条坐标轴设置名称
    plt.xlabel("机组名称")
    plt.ylabel("机组饱和度K值")
    # 显示图例
    plt.xticks(rotation=90)
    plt.legend()
    return fig


def power_visualization(wt_data_dict, wf_data):
    x = list(wt_data_dict.keys())
    y = list(wt_data_dict.values())
    fig, ax = plt.subplots()
    # 绘图
    plt.bar(x=x, height=y, label='推算机组年发电量', color='steelblue', alpha=0.8)
    plt.axhline(y=wf_data, color='red', linestyle='--', label='推算场均年发电量')
    plt.text(x[0], wf_data, f'{wf_data}', color='r', va='top', ha='right', fontsize=12)
    # 在柱状图上显示具体数值, ha参数控制水平对齐方式, va控制垂直对齐方式
    for x1, yy in zip(x, y):
        plt.text(x1, yy + 1, str(yy), ha='center', va='bottom', fontsize=5, rotation=0)
    # 设置标题
    plt.title("推算年发电量图示")
    # 为两条坐标轴设置名称
    plt.xlabel("机组名称")
    plt.ylabel("推算年发电量(万kWh)")
    # 显示图例
    plt.xticks(rotation=90)
    plt.legend()
    return fig


def k_calculate(wt_data, wf_data):
    wt_power_dict, wf_power = power_calculation_plus(wt_data, wf_data)
    k_dict = {}
    for wt_type, wt_power in wt_power_dict.items():
        k_dict[wt_type] = round(wt_power / wf_power, 2)
    k_dict = dict(sorted(k_dict.items()))
    k_fig = k_visualization(k_dict, k_standard=0.95)
    wt_power_dict = dict(sorted(wt_power_dict.items()))
    power_fig = power_visualization(wt_power_dict, wf_power)
    # # 添加电量平均和k值平均
    # wt_power_dict['均值'] = round(np.average(list(wt_power_dict.values())),2)
    # k_dict['均值'] = round(np.average(list(k_dict.values())), 2)
    k_data = pd.DataFrame({'机组名称': k_dict.keys(), '饱和度K': k_dict.values()})
    power_data = pd.DataFrame({'机组名称': wt_power_dict.keys(), '推算年发电量(万kWh)': wt_power_dict.values()})
    result = power_data.merge(k_data, left_on='机组名称', right_on='机组名称', how='inner')
    result.sort_values('机组名称', inplace=True)
    return k_fig, power_fig, result


if __name__ == '__main__':
    standard_air_density = 1.106
    real_air_density = 1.11
    path = r'D:\Personal\桌面\工作任务\5、功率曲线饱和度K值计算\哈密风电场功率曲线一致性系数计算（二月份）.xlsx'
    data = pd.read_excel(path, sheet_name='合同功率曲线')
    df = pd.DataFrame(data)
    df.columns = ['ws', 'power', 'real_power']
    df['convert_ws'] = df['ws'] * math.pow(standard_air_density/real_air_density, 1/3)
    convert_standard_power = []
    for i in range(len(df)-1):
        # 标况风速与折算风速的差值
        w_dif1 = df['ws'].iloc[i] - df['convert_ws'].iloc[i]
        # 合同功率差
        p_dif = df['power'].iloc[i+1]-df['power'].iloc[i]
        # 折算风速差
        w_dif2 = df['convert_ws'].iloc[i+1]-df['convert_ws'].iloc[i]
        # （合同功率差 / 折算风速差 * （标况风速与折算风速的差值）） + 合同功率
        temp = p_dif/w_dif2*w_dif1 + df['power'].iloc[i]
        convert_standard_power.append(temp)
    convert_standard_power.append(None)
    df['convert_standard_power'] = convert_standard_power
    df['rate'] = df['real_power']/df['convert_standard_power']
    rate = np.mean(df['rate'])
    print(df)
    print(rate)

