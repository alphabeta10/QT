"""
国家统计局数据分析
"""
from utils.tool import get_data_from_mongo, sort_dict_data_by
from utils.actions import show_data
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from analysis.alg import judge_peak_lower
from datetime import datetime
# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def cn_st_gdp_cpi_add_rate(time=None):
    """
    获取 GDP增长率 + 通货膨胀率
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False, 'code': True, 'time': True, "data": True}
    # gdp数据有两个  "A010202_jd":"国内生产总值(不变价)累计值(亿元)" 和 "A010102_jd":"国内生产总值(现价)累计值(亿元)"
    code_dict = {'A01020101_yd': '居民消费价格指数(上年同期=100)', "A010202_jd": "国内生产总值(不变价)累计值(亿元)"}
    if time is None:
        time = str(int(datetime.now().strftime("%Y"))-3)
    title = "国家统计局相关月频分析"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    handle_data = []
    for index in data.index:
        ele = dict(data.loc[index])
        time = ele['time']
        if "A" in time:
            time = time.replace("A", "03")
        if "B" in time:
            time = time.replace("B", "06")
        if "C" in time:
            time = time.replace("C", "09")
        if "D" in time:
            time = time.replace("D", "12")
        ele['time'] = time
        handle_data.append(ele)
    new_pd_data = pd.DataFrame(handle_data)
    new_pd_data = pd.pivot_table(new_pd_data, values='data', columns='code', index='time')
    new_pd_data['gdp_growth_rate'] = round(new_pd_data['A010202_jd'].pct_change(12), 6)
    new_pd_data['cpi_growth_rate'] = round((new_pd_data['A01020101_yd'] - 100) / 100, 6)
    new_pd_data['add_value'] = round(new_pd_data['gdp_growth_rate'] + new_pd_data['cpi_growth_rate'], 4)
    new_pd_data.rename(columns={"A010202_jd": "gdp", "A01020101_yd": "cpi"}, inplace=True)
    new_pd_data = new_pd_data.dropna()
    new_json_data = {}
    for index in new_pd_data.index:
        ele = dict(new_pd_data.loc[index])
        new_json_data[index] = ele
    return new_json_data


def industry_cycle_analysis():
    """
    根据gdp和cpi增长相加，得出增长率比较值，初步判断行业属于增长性，防御性，周期性，增长为负
    :return:
    """
    code_dict = {'A020O0903_yd': '工业企业营业收入_累计增长', 'A020O0906_yd': '采矿业营业收入_累计增长',
                 'A020O0909_yd': '煤炭开采和洗选业营业收入_累计增长',
                 'A020O090C_yd': '石油和天然气开采业营业收入_累计增长',
                 'A020O090F_yd': '黑色金属矿采选业营业收入_累计增长',
                 'A020O090I_yd': '有色金属矿采选业营业收入_累计增长', 'A020O090L_yd': '非金属矿采选业营业收入_累计增长',
                 'A020O090O_yd': '开采专业及辅助性活动营业收入_累计增长', 'A020O090R_yd': '其他采矿业营业收入_累计增长',
                 'A020O090U_yd': '制造业营业收入_累计增长', 'A020O090X_yd': '农副食品加工业营业收入_累计增长',
                 'A020O0910_yd': '食品制造业营业收入_累计增长',
                 'A020O0913_yd': '酒、饮料和精制茶制造业营业收入_累计增长',
                 'A020O0916_yd': '烟草制品业营业收入_累计增长', 'A020O0919_yd': '纺织业营业收入_累计增长',
                 'A020O091C_yd': '纺织服装、服饰业营业收入_累计增长',
                 'A020O091F_yd': '皮革、毛皮、羽毛及其制品和制鞋业营业收入_累计增长',
                 'A020O091I_yd': '木材加工和木、竹、藤、棕、草制品业营业收入_累计增长',
                 'A020O091L_yd': '家具制造业营业收入_累计增长', 'A020O091O_yd': '造纸和纸制品业营业收入_累计增长',
                 'A020O091R_yd': '印刷和记录媒介复制业营业收入_累计增长',
                 'A020O091U_yd': '文教、工美、体育和娱乐用品制造业营业收入_累计增长',
                 'A020O091X_yd': '石油、煤炭及其他燃料加工业营业收入_累计增长',
                 'A020O0920_yd': '化学原料和化学制品制造业营业收入_累计增长',
                 'A020O0923_yd': '医药制造业营业收入_累计增长', 'A020O0926_yd': '化学纤维制造业营业收入_累计增长',
                 'A020O0929_yd': '橡胶和塑料制品业营业收入_累计增长',
                 'A020O092C_yd': '非金属矿物制品业营业收入_累计增长',
                 'A020O092F_yd': '黑色金属冶炼和压延加工业营业收入_累计增长',
                 'A020O092I_yd': '有色金属冶炼和压延加工业营业收入_累计增长',
                 'A020O092L_yd': '金属制品业营业收入_累计增长', 'A020O092O_yd': '通用设备制造业营业收入_累计增长',
                 'A020O092R_yd': '专用设备制造业营业收入_累计增长', 'A020O092U_yd': '汽车制造业营业收入_累计增长',
                 'A020O092X_yd': '铁路、船舶、航空航天和其他运输设备制造业营业收入_累计增长',
                 'A020O0930_yd': '电气机械和器材制造业营业收入_累计增长',
                 'A020O0933_yd': '计算机、通信和其他电子设备制造业营业收入_累计增长',
                 'A020O0936_yd': '仪器仪表制造业营业收入_累计增长', 'A020O0939_yd': '其他制造业营业收入_累计增长',
                 'A020O093C_yd': '废弃资源综合利用业营业收入_累计增长',
                 'A020O093F_yd': '金属制品、机械和设备修理业营业收入_累计增长',
                 'A020O093I_yd': '电力、热力、燃气及水生产和供应业营业收入_累计增长',
                 'A020O093L_yd': '电力、热力生产和供应业营业收入_累计增长',
                 'A020O093O_yd': '燃气生产和供应业营业收入_累计增长',
                 'A020O093R_yd': '水的生产和供应业营业收入_累计增长'}

    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    title = "占比"
    sort_key = "time"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$in": ['202308']}}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    compared_rate = cn_st_gdp_cpi_add_rate()['202309']['add_value']*100
    print(compared_rate)
    count_dict = {"lt":[],"gt":[]}
    for index in data.index:
        ele = data.loc[index]
        code = ele['code']
        value = float(ele['data'])
        if value>compared_rate:
            count_dict['gt'].append({code_dict.get(code):value})
        else:
            count_dict['lt'].append({code_dict.get(code):value})
    for k,v in count_dict.items():
        print(k,v)



def cn_st_month_market_analysis(code_dict=None, time=None, title=None, sort_key="time"):
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    if code_dict is None:
        code_dict = {'A020O0913_yd': '酒、饮料和精制茶制造业营业收入累计增长率'}
    if time is None:
        time = "201801"
    if title is None:
        title = "国家统计局相关月频分析"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    year_dict_data = {}
    count_dict = {}
    for index in data.index:
        ele = data.loc[index]
        # print(dict(ele))
        time = ele['time']
        code = code_dict.get(ele['code'])
        val = ele['data']
        year = time[0:4]
        combine_key = f"{year}年{code}"
        if combine_key not in year_dict_data.keys():
            year_dict_data[combine_key] = [0.0] * 12
            count_dict[combine_key] = 0
        year_dict_data[combine_key][count_dict.get(combine_key)] = val
        count_dict[combine_key] = count_dict[combine_key] + 1
    convert_data = pd.DataFrame(data=year_dict_data,
                                index=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月',
                                       '12月'])
    convert_data.plot(kind='bar', title=title, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()


def board_st_month_market_analysis(name=None, unit=None, title=None, val_key=None, data_type=None):
    database = 'govstats'
    collection = 'customs_goods'
    if name is None:
        name = '尿素'
    if unit is None:
        unit = '万吨'
    if data_type is None:
        data_type = "export_goods_detail"
    projection = {'_id': False}
    condition = {"name": name, "data_type": data_type, "unit": unit}
    sort_key = "date"
    if title is None:
        title = '出口数据分析'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    dict_fs = {
        "month_volume_cyc": "当月数量同比",
        "month_volume": "当月数量",
        "acc_month_volume_cyc": "累计当月数量同比",
        "acc_month_volume": "累计当月数量",
        "month_amount_cyc": "当月金额同比",
        "month_amount": "当月金额",
        "acc_month_amount_cyc": "累计当月金额同比",
        "acc_month_amount": "累计当月金额",

    }
    if val_key is None:
        val_key = 'acc_month_amount_cyc'
    if val_key not in dict_fs.keys():
        key_list = list(dict_fs.keys())
        keystr = ",".join(key_list)
        print(f"not val in {keystr}")
        return
    data[list(dict_fs.keys())] = data[list(dict_fs.keys())].astype(float)

    year_dict_data = {}
    for index in data.index:
        ele = data.loc[index]
        print(dict(ele))
        time = ele['date']
        code = ele['name']
        val = ele[val_key]
        year = time[0:4]
        metric = dict_fs.get(val_key)
        combine_key = f"{year}年{code}{metric}"
        index_of = int(time.split("-")[1]) - 1
        if combine_key not in year_dict_data.keys():
            year_dict_data[combine_key] = [0.0] * 12
        year_dict_data[combine_key][index_of] = val
    convert_data = pd.DataFrame(data=year_dict_data,
                                index=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月',
                                       '12月'])
    convert_data.plot(kind='bar', title=title, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()


def cn_st_month_industry_revene_rate_analysis():
    """
    'A020O0901_yd': '工业企业营业收入_累计值',
    'A020O090S_yd': '制造业营业收入_累计值',
    :return:
    """
    code_dict = {'A020O0904_yd': '采矿业营业收入_累计值',
                 'A020O0907_yd': '煤炭开采和洗选业营业收入_累计值', 'A020O090A_yd': '石油和天然气开采业营业收入_累计值',
                 'A020O090D_yd': '黑色金属矿采选业营业收入_累计值', 'A020O090G_yd': '有色金属矿采选业营业收入_累计值',
                 'A020O090J_yd': '非金属矿采选业营业收入_累计值', 'A020O090M_yd': '开采专业及辅助性活动营业收入_累计值',
                 'A020O090P_yd': '其他采矿业营业收入_累计值',
                 'A020O090V_yd': '农副食品加工业营业收入_累计值', 'A020O090Y_yd': '食品制造业营业收入_累计值',
                 'A020O0911_yd': '酒、饮料和精制茶制造业营业收入_累计值', 'A020O0914_yd': '烟草制品业营业收入_累计值',
                 'A020O0917_yd': '纺织业营业收入_累计值', 'A020O091A_yd': '纺织服装、服饰业营业收入_累计值',
                 'A020O091D_yd': '皮革、毛皮、羽毛及其制品和制鞋业营业收入_累计值',
                 'A020O091G_yd': '木材加工和木、竹、藤、棕、草制品业营业收入_累计值',
                 'A020O091J_yd': '家具制造业营业收入_累计值',
                 'A020O091M_yd': '造纸和纸制品业营业收入_累计值', 'A020O091P_yd': '印刷和记录媒介复制业营业收入_累计值',
                 'A020O091S_yd': '文教、工美、体育和娱乐用品制造业营业收入_累计值',
                 'A020O091V_yd': '石油、煤炭及其他燃料加工业营业收入_累计值',
                 'A020O091Y_yd': '化学原料和化学制品制造业营业收入_累计值', 'A020O0921_yd': '医药制造业营业收入_累计值',
                 'A020O0924_yd': '化学纤维制造业营业收入_累计值', 'A020O0927_yd': '橡胶和塑料制品业营业收入_累计值',
                 'A020O092A_yd': '非金属矿物制品业营业收入_累计值',
                 'A020O092D_yd': '黑色金属冶炼和压延加工业营业收入_累计值',
                 'A020O092G_yd': '有色金属冶炼和压延加工业营业收入_累计值', 'A020O092J_yd': '金属制品业营业收入_累计值',
                 'A020O092M_yd': '通用设备制造业营业收入_累计值', 'A020O092P_yd': '专用设备制造业营业收入_累计值',
                 'A020O092S_yd': '汽车制造业营业收入_累计值',
                 'A020O092V_yd': '铁路、船舶、航空航天和其他运输设备制造业营业收入_累计值',
                 'A020O092Y_yd': '电气机械和器材制造业营业收入_累计值',
                 'A020O0931_yd': '计算机、通信和其他电子设备制造业营业收入_累计值',
                 'A020O0934_yd': '仪器仪表制造业营业收入_累计值',
                 'A020O0937_yd': '其他制造业营业收入_累计值', 'A020O093A_yd': '废弃资源综合利用业营业收入_累计值',
                 'A020O093D_yd': '金属制品、机械和设备修理业营业收入_累计值',
                 'A020O093G_yd': '电力、热力、燃气及水生产和供应业营业收入_累计值',
                 'A020O093J_yd': '电力、热力生产和供应业营业收入_累计值',
                 'A020O093M_yd': '燃气生产和供应业营业收入_累计值',
                 'A020O093P_yd': '水的生产和供应业营业收入_累计值'}

    code_dict = {'A020O0J04_yd': '采矿业营业利润_累计值',
                 'A020O0J07_yd': '煤炭开采和洗选业营业利润_累计值', 'A020O0J0A_yd': '石油和天然气开采业营业利润_累计值',
                 'A020O0J0D_yd': '黑色金属矿采选业营业利润_累计值', 'A020O0J0G_yd': '有色金属矿采选业营业利润_累计值',
                 'A020O0J0J_yd': '非金属矿采选业营业利润_累计值', 'A020O0J0M_yd': '开采专业及辅助性活动营业利润_累计值',
                 'A020O0J0P_yd': '其他采矿业营业利润_累计值',
                 'A020O0J0V_yd': '农副食品加工业营业利润_累计值', 'A020O0J0Y_yd': '食品制造业营业利润_累计值',
                 'A020O0J11_yd': '酒、饮料和精制茶制造业营业利润_累计值', 'A020O0J14_yd': '烟草制品业营业利润_累计值',
                 'A020O0J17_yd': '纺织业营业利润_累计值', 'A020O0J1A_yd': '纺织服装、服饰业营业利润_累计值',
                 'A020O0J1D_yd': '皮革、毛皮、羽毛及其制品和制鞋业营业利润_累计值',
                 'A020O0J1G_yd': '木材加工和木、竹、藤、棕、草制品业营业利润_累计值',
                 'A020O0J1J_yd': '家具制造业营业利润_累计值', 'A020O0J1M_yd': '造纸和纸制品业营业利润_累计值',
                 'A020O0J1P_yd': '印刷和记录媒介复制业营业利润_累计值',
                 'A020O0J1S_yd': '文教、工美、体育和娱乐用品制造业营业利润_累计值',
                 'A020O0J1V_yd': '石油、煤炭及其他燃料加工业营业利润_累计值',
                 'A020O0J1Y_yd': '化学原料和化学制品制造业营业利润_累计值', 'A020O0J21_yd': '医药制造业营业利润_累计值',
                 'A020O0J24_yd': '化学纤维制造业营业利润_累计值', 'A020O0J27_yd': '橡胶和塑料制品业营业利润_累计值',
                 'A020O0J2A_yd': '非金属矿物制品业营业利润_累计值',
                 'A020O0J2D_yd': '黑色金属冶炼和压延加工业营业利润_累计值',
                 'A020O0J2G_yd': '有色金属冶炼和压延加工业营业利润_累计值', 'A020O0J2J_yd': '金属制品业营业利润_累计值',
                 'A020O0J2M_yd': '通用设备制造业营业利润_累计值', 'A020O0J2P_yd': '专用设备制造业营业利润_累计值',
                 'A020O0J2S_yd': '汽车制造业营业利润_累计值',
                 'A020O0J2V_yd': '铁路、船舶、航空航天和其他运输设备制造业营业利润_累计值',
                 'A020O0J2Y_yd': '电气机械和器材制造业营业利润_累计值',
                 'A020O0J31_yd': '计算机、通信和其他电子设备制造业营业利润_累计值',
                 'A020O0J34_yd': '仪器仪表制造业营业利润_累计值', 'A020O0J37_yd': '其他制造业营业利润_累计值',
                 'A020O0J3A_yd': '废弃资源综合利用业营业利润_累计值',
                 'A020O0J3D_yd': '金属制品、机械和设备修理业营业利润_累计值',
                 'A020O0J3G_yd': '电力、热力、燃气及水生产和供应业营业利润_累计值',
                 'A020O0J3J_yd': '电力、热力生产和供应业营业利润_累计值',
                 'A020O0J3M_yd': '燃气生产和供应业营业利润_累计值', 'A020O0J3P_yd': '水的生产和供应业营业利润_累计值'}

    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    title = "占比"
    sort_key = "time"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$in": ['201812', '201912', '202012', '202112', '202212', '202307']}}
    index_dict = {"201812": 0, "201912": 1, "202012": 2, "202112": 3, "202212": 4, "202307": 5}
    revert_index_dict = {}
    for k, v in index_dict.items():
        revert_index_dict[v] = k

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    sum_val_dict = {}
    code_val_dict = {}
    rate_code_val_dict = {}
    last_rate_code_val = {}
    for index in data.index:
        ele = data.loc[index]
        time = ele['time']
        code = code_dict.get(ele['code'])
        val = float(ele['data'])
        if code not in code_val_dict.keys():
            num = len(list(index_dict.keys()))
            code_val_dict[code] = [0] * num
            rate_code_val_dict[code] = [0] * num
        code_val_dict[code][index_dict.get(time)] = val
        if time not in sum_val_dict.keys():
            sum_val_dict[time] = 0
        sum_val_dict[time] = sum_val_dict[time] + val
    print(sum_val_dict)
    print(code_val_dict)
    for code, list_val in code_val_dict.items():
        for i, val in enumerate(list_val):
            year = revert_index_dict.get(i)
            sum_val = sum_val_dict.get(year)
            code_rate_val = round(val / sum_val, 4)
            if year == '202307':
                last_rate_code_val[code] = code_rate_val
            rate_code_val_dict[code][i] = code_rate_val
    print(rate_code_val_dict)
    print(last_rate_code_val)
    pd_data = pd.DataFrame(data=rate_code_val_dict, index=list(index_dict.keys()))
    cols = list(sort_dict_data_by(last_rate_code_val, by="value").keys())[-10:]
    pd_data = pd_data[cols]
    print(sort_dict_data_by(last_rate_code_val, by="value"))
    pd_data.plot(kind='bar', title=title, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()


def cn_st_analysis_industry_goods_peak():
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    sort_key = "time"
    code_dict = {"A02090N01_yd":"农用氮、磷、钾化学肥料（折纯）产量当期值(万吨)"}
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['time','strdata']
    data = data[cols]
    combine_dict_data = {}
    for index in data.index:
        ele = data.loc[index]
        time = ele['time']
        strdata = ele['strdata']
        if strdata=='':
            continue
        month = time[4:]
        year = time[0:4]
        if year not in combine_dict_data.keys():
            combine_dict_data[year] = {"month":[],"data":[]}
        combine_dict_data[year]['month'].append(month)
        combine_dict_data[year]['data'].append(float(strdata))

    month_peak_low_st = {"lower":{},"peak":{}}

    def peak_st(month_peak_low_st,index_data,month_list):
        for index,is_peak in index_data.items():
            cur_month = month_list[index]
            if is_peak is True:
                if cur_month not in month_peak_low_st['peak'].keys():
                    month_peak_low_st['peak'][cur_month] = 0
                month_peak_low_st['peak'][cur_month]+= 1
            if is_peak is False:
                if cur_month not in month_peak_low_st['lower'].keys():
                    month_peak_low_st['lower'][cur_month] = 0
                month_peak_low_st['lower'][cur_month]+= 1


    for year,month_data_list in combine_dict_data.items():
        data = month_data_list['data']
        month = month_data_list['month']
        index_data = judge_peak_lower(data)
        print("*"*50)
        print(data)
        print(index_data)
        if len(data)==10:
            cur_month = month[9]
            cur_data = data[9]
            print(f"year={year},month={cur_month},data={cur_data}")
        print("*" * 50)
        peak_st(month_peak_low_st,index_data,month)
    print(month_peak_low_st)

    database = 'stock'
    collection = 'goods'
    projection = {'_id': False}
    sort_key = "time"
    code = '尿素'
    condition = {"name":code,"data_type":"goods_price"}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    combine_dict_data = {}
    for index in data.index:
        time = data.loc[index]['time']
        value = data.loc[index]['value']
        year = time[0:4]
        month = time[4:6]
        if year not in combine_dict_data.keys():
            combine_dict_data[year] = {"month":[],"data":{}}

        if month not in combine_dict_data[year]['data'].keys():
            combine_dict_data[year]['data'][month] = []
        if month not in combine_dict_data[year]['month']:
            combine_dict_data[year]['month'].append(month)
        combine_dict_data[year]['data'][month].append(float(value))

    month_peak_low_st = {"lower":{},"peak":{}}
    for year,combine_data in combine_dict_data.items():
        month_list = combine_data['month']
        month_data = combine_data['data']
        print(month_list)
        month_values = []
        for month,data_list in month_data.items():
            print(month,np.mean(data_list))
            month_values.append(np.mean(data_list))
        index_data = judge_peak_lower(month_values)
        peak_st(month_peak_low_st, index_data,month_list)
    print(month_peak_low_st)

    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False,'time':True,'close':True}
    sort_key = "time"
    code = '000408'
    condition = {"code": code}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    combine_dict_data = {}
    for index in data.index:
        time = data.loc[index]['time']
        value = data.loc[index]['close']
        year = time[0:4]
        month = time[5:7]
        if year not in combine_dict_data.keys():
            combine_dict_data[year] = {"month": [], "data": {}}

        if month not in combine_dict_data[year]['data'].keys():
            combine_dict_data[year]['data'][month] = []
        if month not in combine_dict_data[year]['month']:
            combine_dict_data[year]['month'].append(month)
        combine_dict_data[year]['data'][month].append(float(value))

    month_peak_low_st = {"lower": {}, "peak": {}}
    for year, combine_data in combine_dict_data.items():
        month_list = combine_data['month']
        month_data = combine_data['data']
        print(month_list)
        month_values = []
        for month, data_list in month_data.items():
            print(month, np.mean(data_list))
            month_values.append(np.mean(data_list))
        index_data = judge_peak_lower(month_values)
        peak_st(month_peak_low_st, index_data,month_list)
    lower_rs = month_peak_low_st['lower']
    peak_rs = month_peak_low_st['peak']
    lower_rs = sort_dict_data_by(lower_rs)

    print(f"lower,{lower_rs}")
    print(f"peak,{peak_rs}")
    for k,v in lower_rs.items():
        p = peak_rs.get(k)
        if p is None:
            p = 0
        diff = abs(v-p)
        print(f"month={k},lower={v},peak={p},diff={diff}")


def energy_product_analysis():
    dict_data = {'A03010101_yd': '原煤产量_当期值', 'A03010201_yd': '原油产量_当期值',
                 'A03010301_yd': '天然气产量_当期值', 'A03010401_yd': '煤层气产量_当期值',
                 'A03010501_yd': '液化天然气产量_当期值', 'A03010601_yd': '原油加工量产量_当期值',
                 'A03010701_yd': '汽油产量_当期值', 'A03010801_yd': '煤油产量_当期值',
                 'A03010901_yd': '柴油产量_当期值', 'A03010A01_yd': '燃料油产量_当期值',
                 'A03010B01_yd': '石脑油产量_当期值', 'A03010C01_yd': '液化石油气产量_当期值',
                 'A03010D01_yd': '石油焦产量_当期值', 'A03010E01_yd': '石油沥青产量_当期值',
                 'A03010F01_yd': '焦炭产量_当期值', 'A03010G01_yd': '发电量_当期值',
                 'A03010H01_yd': '火力发电量_当期值', 'A03010I01_yd': '水力发电量_当期值',
                 'A03010J01_yd': '核能发电量_当期值', 'A03010K01_yd': '风力发电量_当期值',
                 'A03010L01_yd': '太阳能发电量_当期值', 'A03010M01_yd': '煤气产量_当期值'}
    dict_data = {"A03010G03_yd": "发电量同比增长_当期值"}  # 发电量很重要
    dict_data = {"A090103_yd": "货运量同比增长_当期值"}  # 货运量同很重要
    dict_data = {"A090303_yd": "客运量同比增长_当期值"}  # 货运量同很重要
    dict_data = {"A070103_yd": "社会消费品零售总额同比增长_当期值"}  # 货运量同很重要
    dict_data = {'A020O0503_yd': '工业企业存货_增减', 'A020O0506_yd': '采矿业存货_增减',
     'A020O0509_yd': '煤炭开采和洗选业存货_增减', 'A020O050C_yd': '石油和天然气开采业存货_增减',
     'A020O050F_yd': '黑色金属矿采选业存货_增减', 'A020O050I_yd': '有色金属矿采选业存货_增减',
     'A020O050L_yd': '非金属矿采选业存货_增减', 'A020O050O_yd': '开采专业及辅助性活动存货_增减',
     'A020O050R_yd': '其他采矿业存货_增减', 'A020O050U_yd': '制造业存货_增减',
     'A020O050X_yd': '农副食品加工业存货_增减', 'A020O0510_yd': '食品制造业存货_增减',
     'A020O0513_yd': '酒、饮料和精制茶制造业存货_增减', 'A020O0516_yd': '烟草制品业存货_增减',
     'A020O0519_yd': '纺织业存货_增减', 'A020O051C_yd': '纺织服装、服饰业存货_增减',
     'A020O051F_yd': '皮革、毛皮、羽毛及其制品和制鞋业存货_增减',
     'A020O051I_yd': '木材加工和木、竹、藤、棕、草制品业存货_增减', 'A020O051L_yd': '家具制造业存货_增减',
     'A020O051O_yd': '造纸和纸制品业存货_增减', 'A020O051R_yd': '印刷和记录媒介复制业存货_增减',
     'A020O051U_yd': '文教、工美、体育和娱乐用品制造业存货_增减', 'A020O051X_yd': '石油、煤炭及其他燃料加工业存货_增减',
     'A020O0520_yd': '化学原料和化学制品制造业存货_增减', 'A020O0523_yd': '医药制造业存货_增减',
     'A020O0526_yd': '化学纤维制造业存货_增减', 'A020O0529_yd': '橡胶和塑料制品业存货_增减',
     'A020O052C_yd': '非金属矿物制品业存货_增减', 'A020O052F_yd': '黑色金属冶炼和压延加工业存货_增减',
     'A020O052I_yd': '有色金属冶炼和压延加工业存货_增减', 'A020O052L_yd': '金属制品业存货_增减',
     'A020O052O_yd': '通用设备制造业存货_增减', 'A020O052R_yd': '专用设备制造业存货_增减',
     'A020O052U_yd': '汽车制造业存货_增减', 'A020O052X_yd': '铁路、船舶、航空航天和其他运输设备制造业存货_增减',
     'A020O0530_yd': '电气机械和器材制造业存货_增减', 'A020O0533_yd': '计算机、通信和其他电子设备制造业存货_增减',
     'A020O0536_yd': '仪器仪表制造业存货_增减', 'A020O0539_yd': '其他制造业存货_增减',
     'A020O053C_yd': '废弃资源综合利用业存货_增减', 'A020O053F_yd': '金属制品、机械和设备修理业存货_增减',
     'A020O053I_yd': '电力、热力、燃气及水生产和供应业存货_增减', 'A020O053L_yd': '电力、热力生产和供应业存货_增减',
     'A020O053O_yd': '燃气生产和供应业存货_增减', 'A020O053R_yd': '水的生产和供应业存货_增减'}

    for k, v in dict_data.items():
        code_dict = {k: v}
        time = "201801"
        title = v.split("_")[0] + "分析"
        cn_st_month_market_analysis(code_dict, time, title)


def rate_gdb():
    country_meta = pd.read_csv("../data/worldbankdata/API_NY.GDP.MKTP.CD_DS2_zh_csv_v2_6000910/Metadata_Country_API_NY.GDP.MKTP.CD_DS2_zh_csv_v2_6000910.csv")
    countrys = ['世界']
    for index in country_meta.index:
        ele = dict(country_meta.loc[index])
        income_group = str(ele['Income_Group'])
        if income_group!='nan':
            countrys.append(ele['Country Name'])
    print(countrys)

    pd_data = pd.read_csv("../data/worldbankdata/API_NY.GDP.MKTP.CD_DS2_zh_csv_v2_6000910/API_NY.GDP.MKTP.CD_DS2_zh_csv_v2_6000910.csv",encoding='utf8')
    #show_data(pd_data)
    cols = ['Country Name','2022']
    data = pd_data[cols]
    data = data[data['Country Name'].isin(countrys)]
    data = data.dropna()
    total = data[data['Country Name']=='世界']['2022'].values[0]
    data['2022_rank'] = round(data['2022']/total,8)
    data.sort_values(by='2022_rank',inplace=True,ascending=False)





if __name__ == '__main__':
    # code_dict = {'A020O0923_yd': '医药制造业营业收入累计增长率'}
    # code_dict = {'A020O0933_yd': '计算机、通信和其他电子设备制造业营业收入累计增长率'}
    code_dict = {'A02090901_yd': '白酒（折65度，商品量）产量当期值(万千升)'}
    #code_dict = {'A02090A01_yd': '啤酒产量当期值(万千升)'}
    #cn_st_month_market_analysis(code_dict=code_dict)
    # cn_st_month_industry_revene_rate_analysis()
    #board_st_month_market_analysis(name='干鲜瓜果及坚果',unit='万吨',title='干鲜瓜果及坚果进口',data_type='import_goods_detail',val_key='month_volume')
    #cn_st_analysis_industry_goods_peak()
    # board_st_month_market_analysis(name='中药材',unit='吨',val_key='acc_month_volume_cyc')
    # board_st_month_market_analysis(name='中药材',unit='吨',val_key='month_volume_cyc')
    #energy_product_analysis()
    board_st_month_market_analysis(val_key='month_volume',name='大豆',data_type='import_goods_detail',title='大豆进口数据')
    # cn_st_month_market_analysis(code_dict={'A02090N01_yd': '农用氮、磷、钾化学肥料（折纯）产量'},title="农用氮、磷、钾化学肥料（折纯）产量",time='201001')
    # cn_st_month_market_analysis(code_dict={'A02090N01_yd': '啤酒产量'},title="农用氮、磷、钾化学肥料（折纯）产量",time='201001')

    #rate_gdb()
    # json_data = cn_st_gdp_cpi_add_rate() # GDP 增长率+ 通货膨胀率
    # print(json_data)
