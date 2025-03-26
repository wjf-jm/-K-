import streamlit as st
import pandas as pd
import numpy as np
import math
import matplotlib
from matplotlib import pyplot as plt
matplotlib.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei']

PowerCurve = {3.0: 15.41, 3.5: 68.54, 4.0: 121.67, 4.5: 183.56, 5.0: 244.26, 5.5: 356.5,
               6.0: 446.2, 6.5: 580.93, 7.0: 706.97, 7.5: 872.96, 8.0: 1049.23, 8.5: 1226.03,
               9.0: 1393.31, 9.5: 1541.67, 10.0: 1693.93, 10.5: 1849.58, 11.0: 1944.77,
               11.5: 1967.33, 12.0: 2000.0, 12.5: 2000.0, 13.0: 2000.0, 13.5: 2000.0,
               14.0: 2000.0, 14.5: 2000.0, 15.0: 2000.0, 15.5: 2000.0, 16.0: 2000.0,
               16.5: 2000.0, 17.0: 2000.0, 17.5: 2000.0, 18.0: 2000.0, 18.5: 2000.0,
               19.0: 2000.0, 19.5: 2000.0, 20.0: 2000.0, 20.5: 2000.0, 21.0: 2000.0,
               21.5: 2000.0, 22.0: 2000.0, 22.5: 2000.0, 23.0: 2000.0, 23.5: 2000.0,
               24.0: 2000.0, 24.5: 2000.0, 25.0: 2010.0}


def cal_main(standard_air_density, real_air_density, power_input):
    power_cur = pd.DataFrame({
        'ws': PowerCurve.keys(),
        'power': PowerCurve.values()
    })
    # df = power_cur.merge(power_input, left_on='ws', right_on='ws', how='left')
    # df.columns = ['ws', 'power', 'real_power']
    df = power_cur.copy(deep=True)
    df['convert_ws'] = df['ws'] * math.pow(standard_air_density / real_air_density, 1 / 3)
    convert_standard_power = []
    for i in range(len(df) - 1):
        # 标况风速与折算风速的差值
        w_dif1 = df['ws'].iloc[i] - df['convert_ws'].iloc[i]
        # 合同功率差
        p_dif = df['power'].iloc[i + 1] - df['power'].iloc[i]
        # 折算风速差
        w_dif2 = df['convert_ws'].iloc[i + 1] - df['convert_ws'].iloc[i]
        # （合同功率差 / 折算风速差 * （标况风速与折算风速的差值）） + 合同功率
        temp = p_dif / w_dif2 * w_dif1 + df['power'].iloc[i]
        convert_standard_power.append(temp)
    convert_standard_power.append(None)
    df['convert_standard_power'] = convert_standard_power

    merge = df.merge(power_input, left_on='ws', right_on='ws', how='right')
    merge['rate'] = merge['real_power'] / merge['convert_standard_power']
    rate = np.mean(merge['rate'])
    return rate


def highlight_low_score_rows(row, check_column='符合率', threshold=0.95):
    """
    此函数用于判断行中的指定列元素是否小于阈值，
    若小于则整行标红，否则不设置样式
    :param row: 输入的行数据
    :param threshold:
    :param check_column:
    :return: 样式列表
    """
    if row[check_column] < threshold:
        return ['background-color: red'] * len(row)
    return [''] * len(row)


def k_visualization(k_dict, k_standard=0.95):
    x = list(k_dict.keys())
    y = list(k_dict.values())
    fig, ax = plt.subplots()
    # 绘图
    plt.plot(x, y, label='机组符合率', color='green', marker='.')
    plt.axhline(y=k_standard, color='red', linestyle='--', label='符合率标准95%')
    # 在柱状图上显示具体数值, ha参数控制水平对齐方式, va控制垂直对齐方式
    for x1, yy in zip(x, y):
        plt.text(x1, yy, str(round(yy, 2)), ha='center', va='bottom', fontsize=5, rotation=0)
    # 设置标题
    plt.title("机组符合率图示")
    # 为两条坐标轴设置名称
    plt.xlabel("机组名称")
    plt.ylabel("机组符合率")
    # 显示图例
    plt.xticks(rotation=90)
    plt.legend()
    return fig


def process_data(uploaded_file, standard_air_density, real_air_density):
    # 处理多个机组工作表组成的Excel表格，针对每个机组都计算相应的符合率
    result = {}
    excel_file = pd.ExcelFile(uploaded_file)
    # 获取所有表名
    sheet_names = excel_file.sheet_names
    # 遍历每个工作表
    for sheet_name in sheet_names:
        # 获取当前工作表的数据
        data = excel_file.parse(sheet_name)
        df = pd.DataFrame(data).astype(float)
        # print(df)
        df.columns = ['ws', 'real_power']
        df.dropna(subset=['real_power'], inplace=True)
        df = df[df['real_power']>0]
        rate = cal_main(standard_air_density, real_air_density, df)
        result[sheet_name] = rate

    return result


if __name__ == '__main__':
    # 设置侧边栏标题
    st.sidebar.title("用户输入")
    # 数值输入
    standard_air_density = st.sidebar.number_input("输入标准空气密度(kg/m\u00b3)",
                                                   value=1.106, step=0.5, format="%f")
    real_air_density = st.sidebar.number_input("输入现场实际空气密度(kg/m\u00b3)",
                                               value=1.11, step=0.5, format="%f")
    # 在侧边栏添加选项
    selected_option = st.sidebar.radio("选择计算类型", ["多台机组计算", "单台机组计算"])
    # 根据选择的选项显示不同界面

    if selected_option == "多台机组计算":
        st.title("多台机组计算界面")
        st.write("这里是多台机组计算的相关内容。")
        uploaded_file = st.file_uploader("请上传 Excel 文件", type=["xlsx", "xls"])
        if uploaded_file is not None:
            try:
                if uploaded_file.name.endswith('.xlsx') | uploaded_file.name.endswith('.xls'):
                    st.write("文件上传成功！")
                    if st.button("处理并可视化"):
                        # 调用自定义处理函数
                        res = process_data(uploaded_file, standard_air_density, real_air_density)
                        fig = k_visualization(res)
                        st.pyplot(fig)

                        res['功率特性平均值'] = np.mean(list(res.values()))
                        res = pd.DataFrame({
                            '机组名称': res.keys(),
                            '符合率': res.values()
                        })
                        formatted_df = res.style.apply(highlight_low_score_rows, axis=1).format({
                            '符合率': '{:.2f}'
                        })
                        st.table(formatted_df)
                else:
                    st.write("上传文件错误，请上传正确形式的文件。")
            except Exception as e:
                st.write(f"处理文件时出现错误: {e}")
        else:
            st.write("请上传一个 Excel 文件。")
    elif selected_option == "单台机组计算":
        st.title("单台机组计算界面")
        col1, col2 = st.columns(2)
        with col1:
            # 表格输入
            data = {
                '风速': np.arange(3, 25.5, 0.5),
                '风机实际功率': [None] * 45
            }
            df = pd.DataFrame(data).astype(float)

            # 在 Streamlit 应用中显示可编辑的表格
            st.write("请填充表格数据：")
            edited_df = st.data_editor(df, num_rows="dynamic")
            edited_df.columns = ['ws', 'real_power']
            edited_df.dropna(subset=['real_power'], inplace=True)
            edited_df = edited_df[edited_df['real_power'] > 0]
        with col2:
            if st.button("处理并可视化"):
                result = cal_main(standard_air_density, real_air_density, edited_df)
                table_result = pd.DataFrame({"机组符合率": [result]})
                formatted_df = table_result.style.format({
                    '机组符合率': '{:.2f}'
                })
                # print(table_result)
                st.table(table_result)


