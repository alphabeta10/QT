"""
公共方法的指标，例如，股指期货多空比
"""
from data.mongodb import get_mongo_table
from datetime import datetime, timedelta
import talib as ta
import pandas as pd


def get_fin_futures_long_short_rate(codes=None, start_time=None, is_fin=True):
    """
    返回最新一条多空比的数据
    :param codes:
    :param start_time:
    :return:
    """
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    if codes is None:
        condition = {"data_type": "futures_long_short_rate", "date": {"$gt": start_time}, "code": {"$regex": "I"}}
    else:
        condition = {"data_type": "futures_long_short_rate", "date": {"$gt": start_time}, "code": {"$in": codes}}
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    result_data = {}
    for ele in futures_basic_info.find(condition, projection={'_id': False}).sort("date"):
        result_data[ele['code']] = {"date": ele['date'], "long_short_rate": ele['long_short_rate']}
    if is_fin:
        count_dict = {}
        total = 0
        low_risk = 0
        high_risk = 0
        code_dict = {
            "IM": "中证1000",
            "IH": "上证50",
            "IC": "中证500",
            "IF": "沪深300",
        }
        cur_time = datetime.now().strftime("%Y%m%d")[2:6]
        for code, val in result_data.items():
            time = code[2:6]
            long_short_rate = val['long_short_rate']
            code_name = code_dict.get(code[0:2], None)
            if int(time) >= int(cur_time) and code_name is not None and str(long_short_rate) != 'nan':
                leve_risk = "无评级"
                if long_short_rate >= 1:
                    leve_risk = "无风险"
                if long_short_rate > 0.93 and long_short_rate < 1:
                    leve_risk = "低风险"
                elif long_short_rate > 0.8 and long_short_rate <= 0.93:
                    leve_risk = "中风险"
                elif long_short_rate < 0.8:
                    leve_risk = "高风险"
                count_dict.setdefault(leve_risk, 0)
                count_dict[leve_risk] += 1
                total += 1

        result = {k: round(v / total, 4) for k, v in count_dict.items()}
        for k, v in result.items():
            if k in ['无风险', '低风险', '无评级']:
                low_risk += v
            if k in ['中风险', '高风险']:
                high_risk += v

        result_data['high_risk'] = high_risk
        result_data['low_risk'] = low_risk
    return result_data


def get_stock_last_dzjy(codes, start_time):
    """
    返回最近的大宗交易数据
    :param codes:
    :param start_time:
    :return:
    """
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    result_data = {}
    for ele in stock_common.find(
            {"data_type": "stock_dzjy", "time": {"$gt": start_time}, "metric_code": {"$in": codes}},
            projection={'_id': False}).sort("time"):
        result_data[ele['metric_code']] = {"dis_rate": round(ele['dis_rate'], 4), "trade_price": ele['trade_price']}

    for code, val in result_data.items():
        dis_rate = val['dis_rate']
        risk_level = "无评级"
        risk_value = 0
        if dis_rate >= 0:
            risk_level = "无风险"
            risk_value = 0
        elif dis_rate > -0.01 and dis_rate < 0:
            risk_level = "小风险"
            risk_value = 0.1
        elif dis_rate > -0.05 and dis_rate <= -0.01:
            risk_level = "低风险"
            risk_value = 0.3
        elif dis_rate > -0.1 and dis_rate <= -0.05:
            risk_level = "中风险"
            risk_value = 0.5
        elif dis_rate <= -0.1:
            risk_level = "高风险"
            risk_value = 0.8
        result_data[code]['risk_level'] = risk_level
        result_data[code]['risk_value'] = risk_value
    return result_data


def eva_market_margin_risk(last_dict_data):
    result_dict = {}
    if last_dict_data['fin_purchase_amount'] > last_dict_data['fin_purchase_amount_ema5'] and last_dict_data[
        'fin_purchase_amount'] > last_dict_data['fin_purchase_amount_ema10']:
        risk_level = "低风险"
    else:
        risk_level = "有风险"
    result_dict.setdefault(risk_level,0)
    result_dict[risk_level] += 1

    if last_dict_data['sec_selling_volume'] < last_dict_data['sec_selling_volume_ema5'] and last_dict_data[
        'sec_selling_volume'] < last_dict_data['sec_selling_volume_ema10']:
        risk_level = "低风险"
    else:
        risk_level = "有风险"
    result_dict.setdefault(risk_level, 0)
    result_dict[risk_level] += 1
    result_dict = {k:round(v/2,4) for k,v in result_dict.items()}
    return result_dict


def get_last_sz_market_margin_indicator():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    start_day = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    datas = []
    for ele in stock_common.find(
            {"data_type": "margin_data", "metric_code": "margin_sz_simple", "time": {"$gt": start_day}},
            projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['sec_selling_volume', 'time', 'fin_purchase_amount']]
    data['sec_selling_volume_ema5'] = ta.EMA(data['sec_selling_volume'], timeperiod=5)
    data['sec_selling_volume_ema10'] = ta.EMA(data['sec_selling_volume'], timeperiod=10)

    data['fin_purchase_amount_ema5'] = ta.EMA(data['fin_purchase_amount'], timeperiod=5)
    data['fin_purchase_amount_ema10'] = ta.EMA(data['fin_purchase_amount'], timeperiod=10)
    last_risk_dict = eva_market_margin_risk(dict(data.tail(1).iloc[0]))
    return data,last_risk_dict


def get_last_sh_market_margin_indicator():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    start_day = (datetime.now() - timedelta(days=100)).strftime("%Y-%m-%d")
    datas = []
    for ele in stock_common.find(
            {"data_type": "margin_data", "metric_code": "margin_sh", "time": {"$gt": start_day}},
            projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['sec_selling_volume', 'time', 'fin_purchase_amount']]
    data['sec_selling_volume_ema5'] = ta.EMA(data['sec_selling_volume'], timeperiod=5)
    data['sec_selling_volume_ema10'] = ta.EMA(data['sec_selling_volume'], timeperiod=10)

    data['fin_purchase_amount_ema5'] = ta.EMA(data['fin_purchase_amount'], timeperiod=5)
    data['fin_purchase_amount_ema10'] = ta.EMA(data['fin_purchase_amount'], timeperiod=10)
    last_risk_dict = eva_market_margin_risk(dict(data.tail(1).iloc[0]))
    return data,last_risk_dict


def get_stock_margin_indicator(code):
    stock_margin_daily = get_mongo_table(database='stock', collection='stock_margin_daily')

    start_day = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    datas = []
    for ele in stock_margin_daily.find(
            {"code": "002602", "time": {"$gt": start_day}},
            projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['sec_selling_volume', 'time', 'fin_purchase_amount']]
    data['sec_selling_volume_ema5'] = ta.EMA(data['sec_selling_volume'], timeperiod=5)
    data['sec_selling_volume_ema10'] = ta.EMA(data['sec_selling_volume'], timeperiod=10)

    data['fin_purchase_amount_ema5'] = ta.EMA(data['fin_purchase_amount'], timeperiod=5)
    data['fin_purchase_amount_ema10'] = ta.EMA(data['fin_purchase_amount'], timeperiod=10)
    last_risk_dict = eva_market_margin_risk(dict(data.tail(1).iloc[0]))
    return data, last_risk_dict


def get_stock_holder_or_reduce_risk(codes,start_time=None):
    """
    获取最近30日股票是否有股东减持或者增持，起始时间默认前30日
    :param codes:
    :param start_time:
    :return:
    """
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"before day {start_time}")
    result_dict_data = {}
    for ele in stock_seq_daily.find({"metric_key": {"$in": codes}, "ann_time": {"$gte": start_time}},
                                    projection={'_id': False}):
        code = ele['metric_key']
        holder_or_reduce = ele['shareholding_change_overweight']
        shareholding_change_outstanding_share_rate = ele['shareholding_change_outstanding_share_rate']
        if shareholding_change_outstanding_share_rate != '':
            shareholding_change_outstanding_share_rate = float(shareholding_change_outstanding_share_rate)
        else:
            shareholding_change_outstanding_share_rate = 0
        if holder_or_reduce == '减持':
            shareholding_change_outstanding_share_rate = -shareholding_change_outstanding_share_rate
        result_dict_data.setdefault(code,0)
        result_dict_data[code] += shareholding_change_outstanding_share_rate
    risk_level = {}
    for k, v in result_dict_data.items():
        if v > 0:
            risk_level[k] = {"risk_level": "无风险", "risk_value": 0}
        if v < 0:
            risk_level[k] = {"risk_level": "有风险", "risk_value": 0.6}
    return result_dict_data,risk_level

if __name__ == '__main__':
    pass
