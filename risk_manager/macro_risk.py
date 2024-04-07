from analysis.macro_analysis import cn_traffic_data_analysis, wci_index_data_analysis, global_pmi_data_analysis
from analysis.market_analysis import board_st_month_market_price_analysis, board_st_month_market_analysis, \
    board_st_month_market_analysis, cn_st_month_market_analysis
from utils.tool import get_data_from_mongo
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def go_market_board():
    name = '铁矿砂及其精矿'
    # name = '纸浆'
    # name = '原油'
    # name = '尿素'
    name = '服装'
    name = '乙二醇'
    # name = '服装及衣着附件'
    data_type = 'export_goods_detail'
    data_type = 'import_goods_detail'
    unit = '万吨'
    board_st_month_market_analysis(val_key='month_amount', unit=unit, name=name, data_type=data_type,
                                   title=f'{name}进口数据')

    board_st_month_market_price_analysis(val_key='acc_price', name=name, data_type=data_type,
                                         title=f'{name}进口累计价格数据', is_show=True)

    board_st_month_market_price_analysis(val_key='cur_price', name=name, data_type=data_type,
                                         title=f'{name}进口当月价格数据', is_show=True)

    board_st_month_market_analysis(name, data_type=data_type, val_key='month_volume')


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


def cn_cpi_risk(is_show=False):
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    code_dict = {'A01010101_yd': '居民消费价格指数(上年同月=100)',
                 'A01020101_yd': '居民消费价格指数(上年同期=100)',
                 'A01030101_yd': '居民消费价格指数(上月=100)',
                 }
    time = "201801"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time, "$lt": '202403'}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    new_data = pd.pivot_table(data, index='time', columns='code', values='data')
    new_data.rename(columns=code_dict, inplace=True)
    # 计算风险
    for col in code_dict.values():
        for i in range(5):
            new_data[f'{col}_pct_{i + 1}'] = new_data[col].pct_change(i + 1)
    last_dict_data = dict(new_data.tail(1).iloc[0])
    detail_last_risk_dict = {}

    for col in code_dict.values():
        ele = last_dict_data[col]
        detail_last_risk_dict.setdefault(col, {})
        if ele < 100:
            detail_last_risk_dict[col]["deflation_risk"] = 1
        else:
            detail_last_risk_dict[col]['deflation_risk'] = 0
        up, down = 0, 0
        for i in range(5):
            if last_dict_data[f'{col}_pct_{i + 1}'] < 0:
                down += 1
            else:
                up += 1
        detail_last_risk_dict[col]['up'] = up
        detail_last_risk_dict[col]['down'] = down

    total_cpi_risk = 0
    for k, combine_dict in detail_last_risk_dict.items():
        total_cpi_risk += combine_dict['deflation_risk'] * 0.7
        total_cpi_risk += round(combine_dict['down'] / 5, 4) * 0.3
    total_cpi_risk = round(total_cpi_risk / 3, 4)

    if is_show:
        new_data.plot(kind='line', title='不同类型cpi数据', rot=45, figsize=(15, 8), fontsize=10)
        show_data(new_data)
        print(detail_last_risk_dict, total_cpi_risk)
        plt.show()
    return detail_last_risk_dict, total_cpi_risk


def cn_pmi_risk(is_show=False, pmi_type=None):
    """
    计算pmi风险，计算逻辑 = 荣枯线风险*.7 + (前五下跌次数/5)*0.3
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
        for i in range(5):
            new_data[f'{col}_pct_{i + 1}'] = new_data[col].pct_change(i + 1)
    last_dict_data = dict(new_data.tail(1).iloc[0])
    detail_last_risk_dict = {}

    for col in code_dict.values():
        ele = last_dict_data[col]
        detail_last_risk_dict.setdefault(col, {})
        if ele < 50:
            detail_last_risk_dict[col]["wing_dry_risk"] = 1
        else:
            detail_last_risk_dict[col]['wing_dry_risk'] = 0
        up, down = 0, 0
        for i in range(5):
            if last_dict_data[f'{col}_pct_{i + 1}'] < 0:
                down += 1
            else:
                up += 1
        detail_last_risk_dict[col]['up'] = up
        detail_last_risk_dict[col]['down'] = down

    total_pmi_risk = 0
    for k, combine_dict in detail_last_risk_dict.items():
        total_pmi_risk += combine_dict['wing_dry_risk'] * 0.7
        total_pmi_risk += round(combine_dict['down'] / 5, 4) * 0.3
    total_pmi_risk = round(total_pmi_risk / 5, 4)
    if is_show:
        new_data.plot(kind='line', title='不同类型pmi数据', rot=45, figsize=(15, 8), fontsize=10)
        show_data(new_data)
        print(detail_last_risk_dict, total_pmi_risk)
        plt.show()
    return detail_last_risk_dict, total_pmi_risk


def gold_price_risk(is_show=False):
    time = "20180101"
    condition = {"name": '黄金', "time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    data.set_index(keys='time', inplace=True)
    data['value_pct7'] = data['value'].pct_change(7)
    data['value_pct14'] = data['value'].pct_change(14)
    data['value_pct30'] = data['value'].pct_change(30)
    if is_show:
        data[['value_pct7', 'value_pct14', 'value_pct30']].plot(kind='line', title='黄金价格增速', rot=45,
                                                                figsize=(15, 8),
                                                                fontsize=10)
        show_data(data)
        plt.show()
    return dict(data.tail(1).iloc[0])


def energy_price_risk(is_show=False):
    time = "20180101"
    condition = {"name": {"$in": ['WTI原油', 'Brent原油']}, "time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    data = pd.pivot_table(data, index='time', columns='name', values='value')
    cols = ['WTI原油', 'Brent原油']
    for col in cols:
        data[f'{col}_value_pct7'] = data[col].pct_change(7)
        data[f'{col}_value_pct14'] = data[col].pct_change(14)
        data[f'{col}_value_pct30'] = data[col].pct_change(30)
    if is_show:
        show_data(data)
        for col in cols:
            data[[f'{col}_value_pct7', f'{col}_value_pct14', f'{col}_value_pct30']].plot(kind='line',
                                                                                         title=f'{col}价格增速', rot=45,
                                                                                         figsize=(15, 8),
                                                                                         fontsize=10)
            plt.show()
    return dict(data.tail(1).iloc[0])


def metal_price_risk(is_show=False):
    time = "20180101"
    goods_list = ['铜', '铝', '锡']
    condition = {"name": {"$in": goods_list}, "time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    data = pd.pivot_table(data, index='time', columns='name', values='value')
    for col in goods_list:
        data[f'{col}_value_pct7'] = data[col].pct_change(7)
        data[f'{col}_value_pct14'] = data[col].pct_change(14)
        data[f'{col}_value_pct30'] = data[col].pct_change(30)
    if is_show:
        show_data(data)
        for col in goods_list:
            data[[f'{col}_value_pct7', f'{col}_value_pct14', f'{col}_value_pct30']].plot(kind='line',
                                                                                         title=f'{col}价格增速', rot=45,
                                                                                         figsize=(15, 8),
                                                                                         fontsize=10)
            plt.show()
    return dict(data.tail(1).iloc[0])


def us_currency_risk(is_show=False,before_num =6):
    """
    美国货币m0增速，增大很可能造成全球通货膨胀，风险计算半年m0的增速大于的个数/6
    :return:
    """
    time = "2018-01-01"
    condition = {"data_name": 'M0', "data_time": {"$gte": time}}
    sort_key = 'data_time'
    database = 'stock'
    collection = 'micro'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.set_index(keys='data_time', inplace=True)
    detail_last_risk = {}
    for i in range(before_num):
        data[f'value_pct_{i + 1}'] = round(data['value'].pct_change(i + 1), 4)

    last_dict_data = dict(data.tail(1).iloc[0])
    up, down = 0, 0
    for i in range(before_num):
        if last_dict_data[f'value_pct_{i + 1}'] > 0:
            up += 1
        else:
            down += 1
    detail_last_risk['up'] = up
    detail_last_risk['down'] = down
    total_last_risk = round(detail_last_risk['up'] / before_num, 4)
    detail_last_risk['total_risk'] = total_last_risk
    if is_show:
        data['value'].plot(kind='line', title='美国m0货币', rot=45, figsize=(15, 8),
                           fontsize=10)
        show_data(data)
        print(detail_last_risk)
        plt.show()
        data[['value_pct_1', 'value_pct_2', 'value_pct_3', 'value_pct_4', 'value_pct_5']].plot(kind='line',
                                                                                               title='美国货币m0同比增长',
                                                                                               rot=45,
                                                                                               figsize=(15, 8),
                                                                                               fontsize=10)
        plt.show()

    return detail_last_risk, total_last_risk


def cn_currency_risk(is_show=False):
    """
    中国货币
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    code_dict = {'A0D0102_yd': '货币和准货币(M2)供应量同比增长(%)',
                 'A0D0104_yd': '货币和准货币(M1)供应量同比增长(%)',
                 'A0D0106_yd': '货币和准货币(M0)供应量同比增长(%)',
                 }
    code_dict = {'A0D0101_yd': 'M2_money',
                 'A0D0102_yd': 'M2',
                 'A0D0104_yd': 'M1',
                 'A0D0106_yd': 'M0'}
    time = "201801"
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
    time = "201801"
    title = "m2和m1以及m0货币数据"
    condition = {"data_type": "fin_monetary", "metric_code": "balance_monetary_authority", "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    data['time1'] = data['time'].apply(lambda ele: ele[0:6])
    data = pd.merge(m_data, data, on=['time1'])
    data['m_multiply'] = round(data['M2_money'] / data['reserve_money'], 4)
    data = data[['M2_money', 'time', 'm2_m1_diff', 'M2', 'M1', 'M0', 'm_multiply']]
    data.set_index(keys='time', inplace=True)
    if is_show:
        data[['m2_m1_diff', 'M2', 'M1', 'M0', 'm_multiply']].plot(kind='line', title=title, rot=45, figsize=(15, 8),
                                                                  fontsize=10)
        show_data(data)
        plt.show()
    return data


def traffic_analysis():
    cn_traffic_data_analysis()
    wci_index_data_analysis()
    global_pmi_data_analysis()


if __name__ == '__main__':
    us_currency_risk(is_show=True)
