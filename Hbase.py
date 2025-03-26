# 从实时库中读取设备秒级数据
import os
import datetime
import json
import time
import pandas as pd
import psutil
import requests
from alive_progress import alive_bar
from loguru import logger
import concurrent.futures

# 数据获取时的并发数量，两种数据不可随意修改，否则会发生内存溢出或未知错误
MAX_PROCESS = 5  # 进程池最大支持进程数量。
MAX_THREAD = 15  # 线程池最大支持线程数量

# HBASE数据下载地址配置
BUSINESS_DATA_URL = "http://10.12.27.3:8082"
appKey = 'HLutl21rHUI0aSkRWWvLJ5/KPEFOR7BsSXCtfztL1UM='


def http_post_restful_data(url, body, *headers):
    """
    基于http_post方式获取数据
    Args:
        url:
        body:
    Returns:
    """
    retry_times = 0
    result = []
    while retry_times <= 3:
        try:
            if len(headers) == 0:
                result = requests.post(url, data=body)
            else:
                result = requests.post(url, data=body, headers=headers[0])
        except Exception as error:
            raise error
        if result.status_code == 200:
            json_content = result.text
            json_data = json.loads(json_content)
            result_data = json_data['data']
            return result_data
        else:
            retry_times += 1
            time.sleep(1)
    raise Exception(
        'Data Services status_code: {}'.format(str(result.status_code)))


def data_processing(data, wt_scada_id):
    temp = pd.DataFrame(data)
    temp = temp.resample('1min').mean()
    temp_mean = temp.reset_index()
    temp_mean.rename(columns={"index": "rectime", "WTUR.WSpd.Ra.F32": "ws", "WTUR.PwrAt.Ra.F32": "power",
                        "WTPS.Ang.Ra.F32.blade1": "pitch"}, inplace=True)
    temp_mean['wt_type'] = wt_scada_id
    # res = temp_mean[(temp_mean['power'] >= 5) & (abs(temp_mean['pitch']) < 5)]
    return temp_mean


def get_business_data(wt_scada_id, start_time, end_time, iec_path_list):
    """
    按设备、逻辑模型、时间方位、iec字段名称获取业务数据
    :param iec_path_list:
    :param wt_scada_id: scada ID
    :param start_time: 数据读取开始时间  "20221205000000"
    :param end_time: 数据读取结束时间  "20221206000000"
    :return: 业务数据
    """
    assert isinstance(wt_scada_id, str), 'device_scada_id不是string'
    assert isinstance(start_time, str), 'start_time 不是string'
    assert isinstance(end_time, str), 'end_time 不是string'
    assert isinstance(iec_path_list, list), 'iec_path_dic 不是 list'
    data_end_time = datetime.datetime.strptime(
        end_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') + '.000'
    data_start_time = datetime.datetime.strptime(
        start_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S') + '.000'
    iec_tags = ''
    for tid in iec_path_list:
        if tid != "" and iec_tags == '':
            iec_tags = '\"' + tid + '\"'
        elif tid != '' and iec_tags != '':
            iec_tags += ',' + '\"' + tid + '\"'
    if iec_tags == "":
        # LOGGER.info("需要获取的字段列表除时间戳外为空！！！！")
        return pd.DataFrame()
    else:
        http_url = ("{}/query/getData".format(BUSINESS_DATA_URL))
        headers = {"content-type": "application/json",
                   "appKey": appKey}
        http_body = ('{\"compress\": false, \"dataCategory\": \"REALDATA\",'
                     '\"endTime\": \"' + data_end_time +
                     '\",\"startTime\": \"' + data_start_time +
                     '\",\"tags\": {\"' + wt_scada_id + '\":[' + iec_tags + ']}}')
        result = http_post_restful_data(http_url, http_body, headers)
        if len(result) == 0:
            return pd.DataFrame()
        else:
            data = {}
            iec_list = result['relaMap'].keys()
            iec_data_len = 0
            for iec in iec_list:
                data_tmp = result['tagValueListMap'][iec]['valueList']
                time_tmp = result['timeCollect'][result['relaMap'][iec]][
                    'timeList']
                time_tmp_1 = [datetime.datetime.fromtimestamp(tmp / 1000) for tmp in time_tmp]
                d_pro = pd.Series(index=time_tmp_1, data=data_tmp,
                                  name=iec.split('.', 1)[1])
                iec_name = iec.replace(wt_scada_id + ".", "")
                data[iec_name] = d_pro
                if iec_data_len == 0:
                    iec_data_len = len(d_pro)
                else:
                    if iec_data_len != len(d_pro):
                        logger.info("%s设备不同的测点间数据长度不同" % wt_scada_id)
            return data_processing(data, wt_scada_id)


def hbase_data_get(wt_list, start_time, end_time, iec_list):
    """
    场站秒级数据读取函数
    :param wt_list: dataframe 场站机组台账信息
    :param iec_list: dict 字段配置字典
    :param start_time: 数据获取开始时间
    :param end_time: 数据获取结束时间
    :return: 数据列表
    """
    data_df = pd.DataFrame()
    with alive_bar(len(wt_list), title='数据获取进度', bar='filling', spinner='waves', force_tty=True) as bar:

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PROCESS) as executor:
            to_do = []
            for wt in wt_list:
                future = executor.submit(get_business_data, wt, start_time, end_time,
                                         iec_list)
                to_do.append(future)
            for future in concurrent.futures.as_completed(to_do):

                data_df = pd.concat((data_df, future.result()), axis=0)
                bar()

    return data_df


if __name__ == "__main__":

    r2 = hbase_data_get(['140601005','140601006'], '2025-01-01 00:00:00', '2025-01-11 23:59:59',
                           ['WTUR.WSpd.Ra.F32', 'WTUR.PwrAt.Ra.F32', 'WTPS.Ang.Ra.F32.blade1'])

    import numpy as np
    print(np.max(r2['ws']))
