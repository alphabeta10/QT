import pandas as pd


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

                if "lt" in combine_ky:
                    if isinstance(combine_dict['lt'], str):
                        compared_val = dict_data[combine_dict['lt']]
                    else:
                        compared_val = combine_dict['lt']
                    if ele_val < compared_val:
                        ret_msg_dict[key] = combine_dict['name']
                if "gt" in combine_ky:
                    if isinstance(combine_dict['gt'], str):
                        compared_val = dict_data[combine_dict['gt']]
                    else:
                        compared_val = combine_dict['gt']
                    if ele_val > compared_val:
                        ret_msg_dict[key] = combine_dict['name']
        if len(ret_msg_dict.keys()) > 0:
            ret_msg_dict['row_data'] = dict_data
    return ret_msg_dict


def comm_indicator_send_msg_by_email(msg_dict_data_list, sender, msg_title='实时股票指标触发',comm_info_dict=None):
    """
    发送邮件
    :param msg_dict_data_list:
    :param sender:
    :param msg_title:
    :param comm_info_dict:
    :return:
    """
    html_msg = "<p>最新监控指标如下有些指标可能触发，请留意</p>"
    html_msg += "<table>"
    html_msg += f"<tr><th>股票相关价格内容</th> <th>触发的指标</th></tr>"
    if comm_info_dict is None:
        comm_info_dict = {"name":"名称","close":"C","open":"O","high":"H","low":"L","pct_chg":"pct_chg"}
    for msg_dict_data in msg_dict_data_list:
        row_data = msg_dict_data['row_data']
        info_msg_list = []
        for info_key,info_name in comm_info_dict.items():
            info_val = row_data[info_key]
            info_msg = f"{info_name}={info_val}"
            info_msg_list.append(info_msg)
        html_info_msg  = ",".join(info_msg_list)

        html_msg += f"<tr><td>{html_info_msg}</td>"
        trigger_list = []
        for k, msg in msg_dict_data.items():
            if k != 'row_data':
                indicator_val = round(row_data[k],2)
                trigger_list.append(msg + f" {k}={indicator_val}")
        trigger_msg = ",".join(trigger_list)
        html_msg += f"<td>{trigger_msg}</td>"
        html_msg += "</tr>"
    html_msg += "</table>"

    if html_msg != '':
        sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], msg_title, html_msg)


if __name__ == '__main__':
    pass
