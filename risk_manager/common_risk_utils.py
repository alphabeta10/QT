import pandas as pd


def comm_down_or_up_risk(data: pd.DataFrame, cal_cols: list, before_num_list: list, col_up_or_down: dict,
                         time_col: str):
    """
    计算下跌或者上升风险方法
    :param data:数据
    :param cal_cols:列名列表
    :param before_num_list:计算的前一个值对比列表
    :param col_up_or_down:上涨还是下跌类型
    :param time_col:
    :return:
    """
    for i in before_num_list:
        for col in cal_cols:
            data[f'{col}_pct_{i}'] = round(data[col].diff(i), 4)
    all_detail_risk = []
    all_datas = []
    for index in data.index:

        detail_risk = {}
        total_risk = 0
        dict_data = dict(data.loc[index])
        if time_col == 'index':
            time = str(index)
        else:
            time = dict_data[time_col]
        for col in cal_cols:
            up, down = 0, 0
            for i in before_num_list:
                if dict_data[f'{col}_pct_{i}'] > 0:
                    up += 1
                else:
                    down += 1
            up_or_down = col_up_or_down.get(col)
            if up_or_down == 'up':
                ele_risk = round(up / len(before_num_list), 4)
            else:
                ele_risk = round(down / len(before_num_list), 4)
            detail_risk[col] = {"up": up, "down": down, "total_risk": ele_risk}
            total_risk += (1 / len(cal_cols)) * ele_risk

        detail_risk['time'] = time
        detail_risk['total_risk'] = total_risk
        all_detail_risk.append(detail_risk)
        dict_data['total_risk'] = total_risk
        dict_data['time'] = time
        all_datas.append(dict_data)
    return all_detail_risk, all_datas
