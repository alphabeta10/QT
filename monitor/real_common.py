import pandas as pd
from datetime import datetime, timedelta
import os


def unique_key_to_file(file_name: str, data_list: set):
    with open(file_name, mode='w') as f:
        for ele in data_list:
            f.write(ele + "\n")


def unique_key_load(file_name: str):
    with open(file_name, mode='r') as f:
        lines = f.readlines()
        keys = set([line.replace("\n", "") for line in lines if line.replace("\n", "") != ''])
        return keys


def common_filter_data(data_dict: dict, file_name, new_key, time_key='time', before_day=3):
    before_day_str = (datetime.now() - timedelta(days=before_day)).strftime("%Y%m%d")
    before_day = int(before_day_str)
    filter_datas = {}
    if os.path.exists(file_name) is False:
        unique_keys = set()
        for name, list_news in data_dict.items():
            new_list_news = []
            for new in list_news:
                day_str = new[time_key].replace("-", "")[0:8]
                day_int = int(day_str)
                if day_int >= before_day:
                    new_list_news.append(new)
                    key = name + new[time_key] + new[new_key]
                    unique_keys.add(key)
            if len(new_list_news) > 0:
                filter_datas[name] = new_list_news
        if len(unique_keys) > 0:
            unique_key_to_file(file_name, unique_keys)
        return filter_datas
    else:
        unique_keys = unique_key_load(file_name)
        new_keys = set()
        is_has_new = False
        for name, list_news in data_dict.items():
            new_list_news = []
            for new in list_news:
                day_str = new[time_key].replace("-", "")[0:8]
                day_int = int(day_str)
                key = name + new[time_key] + new[new_key]

                if day_int >= before_day:
                    new_list_news.append(new)
                    new_keys.add(key)
                    if key not in unique_keys:
                        is_has_new = True
            if len(new_list_news) > 0:
                filter_datas[name] = new_list_news
        if is_has_new:
            unique_key_to_file(file_name, new_keys)
            return filter_datas
        else:
            return {}


def construct_indicator_send_msg(pd_data: pd.DataFrame, indicator_config: dict):
    """
    构建发送邮件的json数据
    :param pd_data:
    :param indicator_config:
    :return:
    """
    ret_msg_dict = {}
    for index in pd_data.index:
        dict_data = dict(pd_data.loc[index])
        show_indicator_set = set()
        for key, combine_dict in indicator_config.items():
            if key in dict_data.keys():
                # 对比数据
                ele_val = dict_data[key]
                combine_ky = combine_dict.keys()
                if "range" in combine_ky:
                    left_val = combine_dict['range'][0]
                    right_val = combine_dict['range'][1]

                    if ele_val > left_val and ele_val < right_val:
                        ret_msg_dict[key] = combine_dict['name']
                        for ele in combine_dict.get('other_show_indicator', []):
                            show_indicator_set.add(ele)
                if "eq" in combine_ky:
                    if ele_val == combine_dict['eq']:
                        ret_msg_dict[key] = combine_dict['name']
                        for ele in combine_dict.get('other_show_indicator', []):
                            show_indicator_set.add(ele)

                if "lt" in combine_ky:
                    if isinstance(combine_dict['lt'], str):
                        compared_val = dict_data[combine_dict['lt']]
                    else:
                        compared_val = combine_dict['lt']
                    if ele_val < compared_val:
                        ret_msg_dict[key] = combine_dict['name']
                        for ele in combine_dict.get('other_show_indicator', []):
                            show_indicator_set.add(ele)
                if "gt" in combine_ky:
                    if isinstance(combine_dict['gt'], str):
                        compared_val = dict_data[combine_dict['gt']]
                    else:
                        compared_val = combine_dict['gt']
                    if ele_val > compared_val:
                        ret_msg_dict[key] = combine_dict['name']
                        for ele in combine_dict.get('other_show_indicator', []):
                            show_indicator_set.add(ele)
        if len(ret_msg_dict.keys()) > 0:
            ret_msg_dict['row_data'] = dict_data
        show_indicator_list = list(show_indicator_set)
        if len(show_indicator_list) > 0:
            ret_msg_dict['other_show_indicator'] = show_indicator_list
    return ret_msg_dict


def comm_indicator_send_msg_by_email(msg_dict_data_list, sender, msg_title='实时股票指标触发', comm_info_dict=None):
    """
    发送邮件
    :param msg_dict_data_list:
    :param sender:
    :param msg_title:
    :param comm_info_dict:
    :return:
    """
    html_msg = "<p>最新监控指标如下有些指标可能触发，请留意</p>"
    html_msg += "<table border=\"1\">"
    html_msg += f"<tr><th>股票相关价格内容</th> <th>触发的指标</th><th>附带指标显示</th></tr>"
    if comm_info_dict is None:
        comm_info_dict = {"name": "名称", "close": "C", "open": "O", "high": "H", "low": "L", "pct_chg": "pct_chg"}
    for msg_dict_data in msg_dict_data_list:
        row_data = msg_dict_data['row_data']
        info_msg_list = []
        for info_key, info_name in comm_info_dict.items():
            info_val = row_data[info_key]
            info_msg = f"{info_name}={info_val}"
            info_msg_list.append(info_msg)
        html_info_msg = ",".join(info_msg_list)

        html_msg += f"<tr><td>{html_info_msg}</td>"
        trigger_list = []
        for k, msg in msg_dict_data.items():
            if k not in ['row_data', 'other_show_indicator']:
                indicator_val = round(row_data[k], 2)
                trigger_list.append(msg + f" {k}={indicator_val}")
        trigger_msg = ",".join(trigger_list)
        html_msg += f"<td>{trigger_msg}</td>"

        trigger_list = []
        if 'other_show_indicator' in msg_dict_data.keys():
            for key in msg_dict_data['other_show_indicator']:
                indicator_val = round(row_data[key], 4)
                trigger_list.append(f"{key}={indicator_val}")
            trigger_msg = ",".join(trigger_list)
            html_msg += f"<td>{trigger_msg}</td>"
        else:
            html_msg += f"<td>无</td>"
        html_msg += "</tr>"
    html_msg += "</table>"

    if html_msg != '':
        sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], msg_title, html_msg)


def st_peak_data(data: pd.DataFrame, time_key, before_peak=-6,before_low=-6):
    data['pre_close'] = data['close'].shift(1)
    data['next_close'] = data['close'].shift(-1)

    data['is_peak'] = data.apply(
        lambda row: 1 if row['close'] > row['pre_close'] and row['close'] > row['next_close'] else 0, axis=1)

    data['is_low'] = data.apply(
        lambda row: 1 if row['close'] < row['pre_close'] and row['close'] < row['next_close'] else 0, axis=1)

    peak_data = []
    low_data = []
    datas = []

    for index in data.index:
        dict_data = dict(data.loc[index])
        compared_list = peak_data[before_peak:]
        low_compared_list = low_data[before_low:]
        cur_close = dict_data['close']
        if time_key not in dict_data.keys():
            time_data = str(index)
        else:
            time_data = dict_data[time_key]
        dict_data[time_key] = time_data
        if dict_data['is_peak'] == 1:
            combine_data = {"close": dict_data['close'], time_key: time_data}
            peak_data.append(combine_data)
        if dict_data['is_low'] == 1:
            combine_data = {"close": dict_data['close'], time_key: time_data}
            low_data.append(combine_data)
        up = 0
        down = 0
        for ele in compared_list:
            tmp_close = ele['close']
            if cur_close > tmp_close:
                up += 1
            else:
                down += 1
        dict_data['up'] = up
        dict_data['down'] = down

        down = 0
        for ele in low_compared_list:
            tmp_close = ele['close']
            if cur_close < tmp_close:
                down += 1
        dict_data['low_down'] = down

        datas.append(dict_data)
    return pd.DataFrame(datas)


if __name__ == '__main__':
    pass
