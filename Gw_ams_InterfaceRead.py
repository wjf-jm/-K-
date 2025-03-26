import os
import datetime
import json
import time
import pandas as pd
import requests
from loguru import logger
import pytz

URL = "https://api.gw-ams.com"
appKey = "b25e39b47e774b4a05b3cb1555fc377f209457c3fd339d373d3fca7b1ea8be56fdc6ed05b7ffb070509bd9293746ae08ea159750efc8fb0842a24c1699406e6c0b02ce81097983b8521c32c4ebc7e0f9b9e8340e40dfc849b537be5c112ce020760f387c5867eae58d0fe4ce4452cf0ee856d0ebc74a68a4fb1d4de430cf12db0e7300d242fb83b52d5dfeac2bc6a58302dedd38fab012c4c2f2c5bb1aee06512deb3d11b9be0e80ea9cbdd85364d1f780f51b73c892027a5c33ab47bee2f25dd472d5aa4f5f286e0349abd6f91d48ac448f36270dc4c98f6fa20f565d7bad04"

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


def http_get_restful_data(url, headers):
    """
    基于http_get方式获取数据
    :param headers: get 请求带参数
    :param url: 请求地址
    :return:
    """
    retry_times = 0
    result = []
    while retry_times <= 3:
        try:
            if len(headers) == 0:
                result = requests.get(url, verify=True)
            else:
                result = requests.get(url, headers=headers, verify=True)
        except Exception as error:
            raise error
        if result.status_code == 200:
            json_content = result.text
            json_data = json.loads(json_content)
            if json_data['result'] != 'error':
                # result_data = json_data['data']
                result_data = json_data
                return result_data
            else:
                retry_times = 4
                logger.info("return code is {} {}!!!".format(
                    str(json_data['statusCode']), str(json_data['message'])))
        else:
            retry_times += 1
            time.sleep(1)
            logger.info("return code is {} {},try {} times !!!".format(
                str(result.status_code), str(result.content), str(retry_times)))
    raise Exception(
        'Data Services status_code: {} {}'.format(str(result.status_code),
                                                  str(result.content)))


def get_token():
    http_url = ("https://api.gw-ams.com/power/token/getToken/we-limitpower.token")

    result = requests.get(http_url)
    if result.status_code == 200:
        json_content = result.text
        json_data = json.loads(json_content)
        result = json_data
    else:
        result = []
    return result["message"]


def get_asset(body):
    """
    从接口获得场站下的AGC设备列表和风机设备列表
    :param body:
    :return:
    """
    http_url = ("{}/ledger/asset/getDeviceInfoByStationDeptNums".format(URL))
    headers = {"content-type": "application/json",
               "loginToken": appKey,
               "accessToken": get_token()}
    http_body = body
    result = http_post_restful_data(http_url, http_body, headers)
    # 获得场站编号及其对应的AGC设备编码和风机编码
    wfinfo_agc = {}
    wfinfo_wt = {}
    for wfid, item in result.items():
        agc = []
        wt = []
        for i in range(len(item)):
            # 如果设备名称等于AGC，则保存AGC编码
            if (item[i]["thirdClass"] == "E0852" and item[i]["assetname"] == "AGC"):
                agc.append(item[i]["assetnum"])
            if (item[i]["thirdClass"] == "G0001"):
                wt.append(item[i]["assetnum"])
        wfinfo_agc[wfid] = agc
        wfinfo_wt[wfid] = wt
    return wfinfo_agc, wfinfo_wt


def get_asset1(wf_id, wf_dept_list,):
    """
    获得场站下的风机设备列表
    :param wf_id:
    :param wf_dept_list,:
    :return:
    """
    # 获取风机数据，thirdClass为分类编码，其中“G0001”为风机的分类编码
    body = {
        "deptNums": wf_dept_list,
        "stationDeptNums": [wf_id],
        "thirdClass": ["G0001"]
    }
    body = json.dumps(body)
    http_url = ("{}/ledger/asset/getDeviceInfoByStationDeptNums".format(URL))
    headers = {"content-type": "application/json",
               "loginToken": appKey}
    http_body = body
    result = http_post_restful_data(http_url, http_body, headers)

    # 获得场站编号及其对应的风机编码
    wfinfo_wt = {}
    if result:
        for wfid, item in result.items():
            wt = []
            for i in range(len(item)):
                wt.append(item[i]["assetnum"])
            wfinfo_wt[wfid] = wt
    else:
        wfinfo_wt[wf_id] = get_asset_by_dept(wf_dept_list)

    return wfinfo_wt


def get_asset2(body):
    """
    获得场站下的风机设备列表
    :param body:
    :return:
    """
    http_url = ("{}/ledger/asset/getDeviceInfoByStationDeptNums".format(URL))
    headers = {"content-type": "application/json",
               "loginToken": appKey,
               "accessToken": get_token()}
    http_body = body
    result = http_post_restful_data(http_url, http_body, headers)

    # 获得场站编号及其对应的风机编码
    wt = []
    for wfid, item in result.items():
        for i in range(len(item)):
            wt.append((item[i]["deptNum"], item[i]["assetnum"]))
    return wt


def get_asset_by_dept(dept_list):
    """
    获得场站下的风机设备列表
    :param body:
    :return:
    """
    wt_df = pd.DataFrame()
    for dept in dept_list:
        http_url = ("{}/ledger/asset/list/{}".format(URL, dept))
        headers = {"content-type": "application/json",
                   "loginToken": appKey,
                   "accessToken": get_token()}
        result = http_get_restful_data(http_url, headers=headers)
        df = pd.DataFrame(result["deceiveNums"])
        wt_df = pd.concat((wt_df, df), axis=0)

    return wt_df


def wf_info_get(wf_list):
    """
    根据场站eam编号获取场站设备列表
    :param eam_id:
    :return: dataframe 风机设备信息
    """
    wfinfo = {}
    for i in range(len(wf_list)):
        eam_id = wf_list[i]
        headers = {"content-type": "application/json"}
        http_url = (
            "{}/power/dept/getDeptByDeptNum//{}".format(
                URL, eam_id))
        result = http_get_restful_data(http_url, headers=headers)
        count = result["deptBody"]["fdczjCount"]
        capacity = result["deptBody"]["contractCapacity"]

        wfinfo[eam_id] = [count, capacity]
    return wfinfo


def get_time_data(body):
    http_url = ("{}/data-common/iotData-common/getTimeData".format(URL))
    headers = {"content-type": "application/json",
               "loginToken": appKey,
               "accessToken": get_token()}
    http_body = body
    result = requests.post(http_url, http_body, headers=headers)
    if result.status_code == 200:
        json_content = result.text
        json_data = json.loads(json_content)
        result = json_data
    else:
        result = []
    return result

def get_historical_data(body):
    http_url = ("{}/data-common/iotData-common/getHistoryData".format(URL))
    headers = {"content-type": "application/json",
               "loginToken": appKey,
               "accessToken": get_token()}
    http_body = body
    result = requests.post(http_url, http_body, headers=headers)
    if result.status_code == 200:
        json_content = result.text
        json_data = json.loads(json_content)
        result = json_data
    else:
        result = []
    return result

def device_data_get(deviceNum, startTime, endTime, label):
    """
    设备数据获取
    :param deviceNum: 设备列表
    :param startTime: 起始日期，格式为"2024-11-29 00:00:00"
    :param endTime: 结束日期，格式为"2024-11-30 00:00:00"
    :param label: 想要获取的指标字段
    :return:
    """
    # 时区转换
    # shanghai_tz = pytz.timezone('Asia/Shanghai')
    # start = pd.to_datetime(startTime)
    # end = pd.to_datetime(endTime)
    # start = int(start.tz_localize('UTC').astimezone(shanghai_tz).timestamp()*1000)
    # end = int(end.tz_localize('UTC').astimezone(shanghai_tz).timestamp() * 1000)

    # start = pd.to_datetime(startTime).strftime("%Y-%m-%d %H:%M:%S")
    # end = pd.to_datetime(endTime).strftime("%Y-%m-%d %H:%M:%S")
    start = startTime
    end = endTime
    body = {
        "deviceNum": deviceNum,
        "endTime": end,
        "label": label,
        "scope": "all",
        "startTime": start
    }
    body = json.dumps(body, indent=2)
    data = get_historical_data(body)
    # for i in range(len(deviceNum)):
    #     device = deviceNum[i]
    #     for j in range(len(data)):
    #         data[j]["assetNum"]

    return data


def get_data2(wfinfo_agc, startTime, endTime, label):
    """
    调用接口为全部的历史数据接口，除了以下参数还可选择查询粒度1min/5min/15min
    :param wfinfo_agc: 场站及对应AGC设备编码的字典
    :param startTime: 开始时间，格式为"2024-11-29 00:00:00"
    :param endTime: 结束时间，格式为"2024-12-03 00:00:00"
    :param label: 测点
    :param scope: 数据完整度，可用值:all,head,tail,avg,stddev,distinct,max,min
    :return:
    """
    # start_time = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    # end_time = datetime.datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S")
    # start = int(start_time.timestamp() * 1000)
    # end = int(end_time.timestamp() * 1000)
    start = pd.to_datetime(startTime).strftime("%Y-%m-%d %H:%M:%S")
    end = pd.to_datetime(endTime).strftime("%Y-%m-%d %H:%M:%S")

    result = {}
    for wf_id, agc_id in wfinfo_agc.items():
        deviceNum = agc_id
        if len(deviceNum) != 0:
            body = {
                "deviceNum": deviceNum,
                "endTime": end,
                "label": label,
                "startTime": start
            }
            body = json.dumps(body, indent=2)
            agc_data = get_historical_data(body)
            # print(agc_data)
            result[wf_id] = agc_data

    return result


def get_limit_power_data(wf_list, startTime, endTime):
    # 获得场站及对应并网容量数据
    wfinfo = wf_info_get(wf_list)
    # 获得场站及对应AGV、风机编码数据
    body = {
        "stationDeptNums": wf_list,
        "thirdClass": ["E0852", "G0001"]
    }
    body = json.dumps(body)
    wfinfo_agc, wfinfo_wt = get_asset(body)

    # 获得场站及AGV相关指标数据
    wfinfo_agcData = get_data2(wfinfo_agc, startTime=startTime, endTime=endTime,
                               label=["AGC_theory_P", "AGC_P_cmd", "AGC_available_P", "AGC_inter_P", "AGC_state"])

    # 获得场站各风机的限电率情况
    wfinfo_wtData = get_data2(wfinfo_wt, startTime=startTime, endTime=endTime,
                              label=["status_p_limit"])
    return wfinfo, wfinfo_wt, wfinfo_wtData, wfinfo_agc, wfinfo_agcData


if __name__ == "__main__":

    wf_dept_list = ['0100100101', '0100100102']
    wf_id = '01001001'
    wfinfo_wt = device_data_get(['G0010010001'], '1740034399000', '1741330399000',
                        ["wind_speed", "P"])
    print(wfinfo_wt)

    result = get_asset_by_dept(wf_dept_list)
    print(result)
