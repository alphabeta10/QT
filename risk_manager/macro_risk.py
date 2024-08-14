from analysis.macro_analysis import cn_traffic_data_analysis, cn_wci_index_data_analysis, global_pmi_data_analysis
from analysis.market_analysis import cn_st_month_market_analysis
from utils.tool import get_data_from_mongo
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def cn_st_metric_risk():
    """
    国家统计局工业指标数据
    :return:
    """
    code_dict = {"A020A0K_yd": "负债合计本月末(亿元)"}
    code_dict = {"A020A0M_yd": "负债合计增减(%)"}

    code_dict = {"A020A1D_yd": "营业利润累计增长(%)"}
    code_dict = {"A020A1B_yd": "营业利润累计值(亿元)"}

    code_dict = {"A020A0B_yd": "存货本月末(亿元)"}
    code_dict = {"A020A0D_yd": "存货增减(%)"}

    code_dict = {"A020A22_yd": "平均用工人数累计值(万人)"}
    code_dict = {"A020A24_yd": "平均用工人数累计增长(%)"}

    code_dict = {"A020A1T_yd": "营业成本累计值(亿元)"}
    code_dict = {"A020A1V_yd": "营业成本累计增长(%)"}

    cn_st_month_market_analysis(code_dict)


def cn_cpi_risk(is_show=False, before_num=6):
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    code_dict = {'A01010101_yd': '居民消费价格指数(上年同月=100)',
                 'A01020101_yd': '居民消费价格指数(上年同期=100)',
                 'A01030101_yd': '居民消费价格指数(上月=100)',
                 }
    time = (datetime.now() - timedelta(days=365)).strftime("%Y-01-01")
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    new_data = pd.pivot_table(data, index='time', columns='code', values='data')
    new_data.rename(columns=code_dict, inplace=True)
    new_data = new_data.dropna()
    # 计算风险
    for col in code_dict.values():
        for i in range(before_num):
            new_data[f'{col}_pct_{i + 1}'] = new_data[col].pct_change(i + 1)
    datas = []
    detail_risks = []
    for index in new_data.index:
        dict_data = dict(new_data.loc[index])
        detail_risk_dict = {}

        for col in code_dict.values():
            ele = dict_data[col]
            detail_risk_dict.setdefault(col, {})
            if ele < 100:
                detail_risk_dict[col]["deflation_risk"] = 1
            else:
                detail_risk_dict[col]['deflation_risk'] = 0
            up, down = 0, 0
            for i in range(5):
                if dict_data[f'{col}_pct_{i + 1}'] < 0:
                    down += 1
                else:
                    up += 1
            detail_risk_dict[col]['up'] = up
            detail_risk_dict[col]['down'] = down

        total_cpi_risk = 0
        for k, combine_dict in detail_risk_dict.items():
            total_cpi_risk += combine_dict['deflation_risk'] * 0.7
            total_cpi_risk += round(combine_dict['down'] / before_num, 4) * 0.3
        total_cpi_risk = round(total_cpi_risk / len(code_dict.keys()), 4)
        detail_risk_dict['total_risk'] = total_cpi_risk
        detail_risk_dict['time'] = str(index)
        dict_data['total_risk'] = total_cpi_risk
        dict_data['time'] = str(index)
        datas.append(dict_data)
        detail_risks.append(detail_risk_dict)
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='cpi风险', rot=45, figsize=(15, 8), fontsize=10)
        show_data(new_data)
        plt.show()
    return detail_risks[-1]


def cn_pmi_risk(is_show=False, pmi_type=None, before_num=6):
    """
    计算pmi风险，计算逻辑 = 荣枯线风险*.7 + (前五下跌次数/5)*0.3
    :param before_num:
    :param is_show:
    :param pmi_type:
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}

    if pmi_type is None:
        code_dict = {'A0B0101_yd': '制造业采购经理指数(%)',
                     'A0B0102_yd': '生产指数(%)',
                     'A0B0103_yd': '新订单指数(%)',
                     'A0B0104_yd': '新出口订单指数(%)',
                     'A0B0301_yd': '综合PMI产出指数(%)',
                     }
    else:
        if pmi_type == 1:
            # 制造业
            code_dict = {'A0B0101_yd': '制造业采购经理指数(%)',
                         'A0B0102_yd': '生产指数(%)',
                         'A0B0103_yd': '新订单指数(%)',
                         'A0B0104_yd': '新出口订单指数(%)',
                         'A0B0301_yd': '综合PMI产出指数(%)',
                         }
        elif pmi_type == 2:
            # 非制作业
            code_dict = {
                'A0B0201_yd': '非制造业商务活动指数(%)',
                'A0B0202_yd': '生产指数(%)',
                'A0B0203_yd': '新订单指数(%)',
                'A0B0204_yd': '新出口订单指数(%)',
                'A0B0301_yd': '综合PMI产出指数(%)',
            }
        else:
            print("pmi类型错误")
            return
    time = "201801"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    new_data = pd.pivot_table(data, index='time', columns='code', values='data')
    new_data.rename(columns=code_dict, inplace=True)
    for col in code_dict.values():
        for i in range(before_num):
            new_data[f'{col}_pct_{i + 1}'] = new_data[col].pct_change(i + 1)

    datas = []
    detail_risks = []
    for index in new_data.index:

        dict_data = dict(new_data.loc[index])
        detail_risk_dict = {}

        for col in code_dict.values():
            ele = dict_data[col]
            detail_risk_dict.setdefault(col, {})
            if ele < 50:
                detail_risk_dict[col]["wing_dry_risk"] = 1
            else:
                detail_risk_dict[col]['wing_dry_risk'] = 0
            up, down = 0, 0
            for i in range(before_num):
                if dict_data[f'{col}_pct_{i + 1}'] < 0:
                    down += 1
                else:
                    up += 1
            detail_risk_dict[col]['up'] = up
            detail_risk_dict[col]['down'] = down

        total_pmi_risk = 0
        for k, combine_dict in detail_risk_dict.items():
            total_pmi_risk += combine_dict['wing_dry_risk'] * 0.7
            total_pmi_risk += round(combine_dict['down'] / before_num, 4) * 0.3
        total_pmi_risk = round(total_pmi_risk / len(code_dict.keys()), 4)
        detail_risk_dict['total_risk'] = total_pmi_risk
        detail_risk_dict['time'] = str(index)
        dict_data['total_risk'] = total_pmi_risk
        dict_data['time'] = str(index)
        detail_risks.append(detail_risk_dict)
        datas.append(dict_data)
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='pmi风险', rot=45, figsize=(15, 8), fontsize=10)
        show_data(new_data)
        plt.show()
    return detail_risks[-1], new_data


def goods_inflation_risk(is_show=False, goods_list=None):
    """
    商品通胀风险
    :param is_show:
    :return:
    """
    time = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    if goods_list is None:
        goods_list = ['铜', '铝', '锡', 'WTI原油', 'Brent原油', '氧化镝', '金属镝', '镨钕氧化物']
    condition = {"name": {"$in": goods_list}, "time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    data = pd.pivot_table(data, index='time', columns='name', values='value')
    pct_list = [3, 7, 14, 30, 50]
    for col in goods_list:
        for num in pct_list:
            data[f'{col}_pct_{num}'] = data[col].pct_change(num)
    all_detail_risk_list = []
    all_data_list = []
    for index in data.index:
        dict_data = dict(data.loc[index])
        detail_risk = {}
        total_inflation_risk = 0
        for col in goods_list:
            up, down = 0, 0
            for num in pct_list:
                if dict_data[f'{col}_pct_{num}'] > 0:
                    up += 1
                else:
                    down += 1
            detail_risk[col] = {"up": up, "down": down, "inflation_risk": round(up / len(pct_list), 4)}
            total_inflation_risk += round(up / len(pct_list), 4)
        total_inflation_risk = round(total_inflation_risk / len(goods_list), 4)
        detail_risk['total_inflation_risk'] = total_inflation_risk
        detail_risk['time'] = str(index)
        dict_data['total_inflation_risk'] = total_inflation_risk
        dict_data['time'] = str(index)
        all_detail_risk_list.append(detail_risk)
        all_data_list.append(dict_data)
    new_data = pd.DataFrame(all_data_list)
    new_data.set_index(keys='time', inplace=True)
    if is_show:
        show_data(new_data)
        new_data['total_inflation_risk'].plot(kind='line', title=f'通胀风险', rot=45, figsize=(15, 8), fontsize=10)
        plt.show()
    return all_data_list[-1], all_detail_risk_list[-1], new_data


def us_currency_risk(is_show=False, before_num=6):
    """
    美国货币m0增速，增大很可能造成全球通货膨胀，风险计算半年m0的增速大于的个数/6
    :return:
    """
    time = (datetime.now() - timedelta(days=365)).strftime("%Y-01-01")
    condition = {"data_name": 'M0', "data_time": {"$gte": time}}
    sort_key = 'data_time'
    database = 'stock'
    collection = 'micro'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.set_index(keys='data_time', inplace=True)
    for i in range(before_num):
        data[f'value_pct_{i + 1}'] = round(data['value'].pct_change(i + 1), 4)
    all_detail_risk = []
    all_datas = []
    for index in data.index:
        detail_last_risk = {}
        dict_data = dict(data.loc[index])
        up, down = 0, 0
        for i in range(before_num):
            if dict_data[f'value_pct_{i + 1}'] > 0:
                up += 1
            else:
                down += 1
        detail_last_risk['up'] = up
        detail_last_risk['down'] = down
        total_last_risk = round(detail_last_risk['up'] / before_num, 4)
        detail_last_risk['total_risk'] = total_last_risk
        detail_last_risk['time'] = str(index)
        all_detail_risk.append(detail_last_risk)
        dict_data['time'] = str(index)
        dict_data['total_risk'] = total_last_risk
        dict_data['up'] = up
        dict_data['down'] = down
        all_datas.append(dict_data)
    new_data = pd.DataFrame(all_datas)
    new_data.set_index(keys='time', inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='美国m0货币超发风险', rot=45, figsize=(15, 8), fontsize=10)
        show_data(data)
        plt.show()
    return all_detail_risk[-1], all_datas[-1], new_data


def cn_currency_risk(is_show=False, before_num=6):
    """
    中国货币
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    code_dict = {'A0D0101_yd': 'M2_money',
                 'A0D0102_yd': 'M2',
                 'A0D0104_yd': 'M1',
                 'A0D0106_yd': 'M0'}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y01")

    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    m_data = pd.pivot_table(data, values='data', index='time', columns='code')
    m_data.rename(columns=code_dict, inplace=True)
    m_data['time1'] = m_data.index
    m_data['m2_m1_diff'] = round(m_data['M2'] - m_data['M1'], 4)

    database = 'stock'
    collection = 'common_seq_data'
    projection = {'_id': False, 'reserve_money': True, 'time': True}
    condition = {"data_type": "fin_monetary", "metric_code": "balance_monetary_authority", "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    data['time1'] = data['time'].apply(lambda ele: ele[0:6])
    data = pd.merge(m_data, data, on=['time1'])
    data['m_multiply'] = round(data['M2_money'] / data['reserve_money'], 4)
    data = data[['M2_money', 'time', 'm2_m1_diff', 'M2', 'M1', 'M0', 'm_multiply']]

    cal_cols = ['m2_m1_diff', 'm_multiply']
    for i in range(before_num):
        for col in cal_cols:
            data[f'{col}_pct_{i + 1}'] = round(data[col].diff(i + 1), 4)
    data = data.dropna()
    all_detail_risk = []
    all_datas = []
    for index in data.index:
        detail_risk = {}
        total_risk = 0
        dict_data = dict(data.loc[index])

        for col in cal_cols:
            up, down = 0, 0
            for i in range(before_num):
                if dict_data[f'{col}_pct_{i + 1}'] > 0:
                    up += 1
                else:
                    down += 1
            if col == 'm_multiply':
                ele_risk = round(up / before_num, 4)
            else:
                ele_risk = round(down / before_num, 4)
            detail_risk[col] = {"up": up, "down": down, "total_risk": ele_risk}
            total_risk += (1 / len(cal_cols)) * ele_risk
        detail_risk['time'] = str(index)
        all_detail_risk.append(detail_risk)
        dict_data['total_risk'] = total_risk
        all_datas.append(dict_data)
    new_data = pd.DataFrame(all_datas)
    new_data.set_index(keys='time', inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='中国货币风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return all_detail_risk[-1], all_datas[-1], new_data


def comm_down_or_up_risk(data: pd.DataFrame, cal_cols: list, before_num_list: list, col_up_or_down: dict,
                         time_col: str):
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


def cn_traffic_risk(is_show=False):
    database = 'stock'
    collection = 'common_seq_data'
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y0101")

    condition = {"data_type": "traffic", "metric_code": "traffic", "time": {"$gt": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    traffic_mapping_dict = {
        "tl_traffic": "铁路运输(万吨)",
        "gk_traffic": "港口吞吐量(万吨)",
        "gk_teu_traffic": "港口集装箱吞吐量(万标箱)",
        "gs_traffic": "货车通行(万辆)",
        "lj_traffic": "邮政揽件",
        "td_traffic": "邮政投递",
    }
    traffic_up_or_down_dict = {
        "tl_traffic": "down",
        "gk_traffic": "down",
        "gk_teu_traffic": "down",
        "gs_traffic": "down",
        "lj_traffic": "down",
        "td_traffic": "down",
    }
    convert_type_col = list(traffic_mapping_dict.keys())
    for col in convert_type_col:
        data[[col]] = data[[col]].astype(float)
    risks, datas = comm_down_or_up_risk(data, list(traffic_mapping_dict.keys()), [1, 2, 3, 4, 5, 6],
                                        traffic_up_or_down_dict, 'time')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='运输风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data


def cn_global_wci_risk(is_show=False):
    database = 'stock'
    collection = 'common_seq_data'
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y0101")
    wci_index_mapping_dict = {"综合指数": "综合指数",
                              "欧洲航线": "欧洲航线",
                              "美西航线": "美西航线",
                              "地中海航线": "地中海航线",
                              "美东航线": "美东航线",
                              "波红航线": "波红航线",
                              "澳新航线": "澳新航线",
                              "西非航线": "西非航线",
                              "南非航线": "南非航线",
                              "南美航线": "南美航线",
                              "东南亚航线": "东南亚航线",
                              "日本航线": "日本航线",
                              "韩国航线": "韩国航线"
                              }
    wic_up_or_down_dict = {
    }
    condition = {"data_type": "cn_wci_index", "metric_code": {"$in": list(wci_index_mapping_dict.keys())},
                 "time": {"$gt": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    convert_type_col = ['cur_month_data']
    for col in convert_type_col:
        data[col] = data[col].astype(float)

    new_data = pd.pivot_table(data, index='time', values='cur_month_data', columns='metric_code')

    risks, datas = comm_down_or_up_risk(new_data, list(wci_index_mapping_dict.keys()), [1, 2, 3, 4, 5, 6],
                                        wic_up_or_down_dict, 'index')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='运输风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data


def cn_electric_risk(is_show=False):
    """
    发电量下滑风险
    :param is_show:
    :return:
    """

    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    code_dict = {'A03010G04_yd': '发电增长率'}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y01")
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data['data'] = data['data'].astype(float)
    data['month'] = data['time'].apply(lambda row:int(row[4:6]))
    data = data[data['month']>=2]
    up_or_down_dict = {}
    new_data = pd.pivot_table(data, index='time', columns='code', values='data')
    risks, datas = comm_down_or_up_risk(new_data, list(code_dict.keys()), [1, 2, 3, 4, 5, 6],
                                        up_or_down_dict, 'index')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='发电量下滑风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data

def cn_fin_risk(is_show=False):
    """
    中国社融减少风险
    :return:
    """
    database = 'stock'
    collection = 'common_seq_data'
    cols = ['afre','rmb_loans','gov_bonds','net_fin_cor_bonds']
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365*2)).strftime("%Y0101")
    condition = {"data_type":"credit_funds", "time": {"$gte": time},"metric_code":"agg_fin_flow"}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.set_index(keys='time',inplace=True)
    data = data[cols]
    data[cols] = data[cols].astype(float)
    data = data.pct_change(12)
    data.dropna(inplace=True)
    risks, datas = comm_down_or_up_risk(data, cols, [1, 2, 3, 4, 5, 6],
                                        {}, 'index')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='社融风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data


def cn_board_risk(is_show=False):
    """
    中国进出口数据风险
    :return:
    """
    database = 'govstats'
    collection = 'customs_goods'
    cols = ['acc_export_import_amount_cyc','acc_export_amount_cyc','acc_import_amount_cyc']
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365*2)).strftime("%Y-01-01")
    condition = {"data_type":"country_export_import","name":"总值", "date": {"$gte": time}}
    sort_key = 'date'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.set_index(keys='date',inplace=True)
    data = data[cols]
    data[cols] = data[cols].astype(float)
    risks, datas = comm_down_or_up_risk(data, cols, [1, 2, 3, 4, 5, 6],
                                        {}, 'index')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='进出口风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data


def cn_global_week_wci_risk(is_show=False):
    database = 'stock'
    collection = 'common_seq_data'
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y-01-01")
    wci_index_mapping_dict = {"中国出口集装箱运价综合指数": "中国出口集装箱运价综合指数",
                              "欧洲航线": "欧洲航线",
                              "美西航线": "美西航线",
                              "地中海航线": "地中海航线",
                              "美东航线": "美东航线",
                              "波红航线": "波红航线",
                              "澳新航线": "澳新航线",
                              "东西非航线": "东西非航线",
                              "南非航线": "南非航线",
                              "南美航线": "南美航线",
                              "东南亚航线": "东南亚航线",
                              "日本航线": "日本航线",
                              "韩国航线": "韩国航线"
                              }
    wic_up_or_down_dict = {
    }
    condition = {"data_type": "cn_ccfi", "metric_code": {"$in": list(wci_index_mapping_dict.keys())},
                 "time": {"$gt": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    convert_type_col = ['data']
    for col in convert_type_col:
        data[col] = data[col].astype(float)

    new_data = pd.pivot_table(data, index='time', values='data', columns='metric_code')

    risks, datas = comm_down_or_up_risk(new_data, list(wci_index_mapping_dict.keys()), [1, 2, 3, 4, 5, 6],
                                        wic_up_or_down_dict, 'index')
    new_data = pd.DataFrame(datas)
    new_data.set_index(keys='time', inplace=True)
    new_data.dropna(inplace=True)
    if is_show:
        new_data['total_risk'].plot(kind='line', title='运输风险', rot=45, figsize=(15, 8),
                                    fontsize=10)
        show_data(new_data)
        plt.show()
    return risks[-1], datas[-1], new_data


def traffic_analysis():
    cn_traffic_data_analysis()
    cn_wci_index_data_analysis()
    global_pmi_data_analysis()


if __name__ == '__main__':
    cn_board_risk(is_show=True)
