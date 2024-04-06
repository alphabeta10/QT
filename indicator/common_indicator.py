"""
公共方法的指标，例如，股指期货多空比
"""
from data.mongodb import get_mongo_table
from datetime import datetime, timedelta
import talib as ta
import pandas as pd
from utils.tool import *


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
            if time != '' and int(time) >= int(cur_time) and code_name is not None and str(long_short_rate) != 'nan':
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


def get_stock_last_dzjy(codes, start_time=None):
    """
    返回最近的大宗交易数据
    :param codes:
    :param start_time:
    :return:
    """
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

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
    result_dict.setdefault(risk_level, 0)
    result_dict[risk_level] += 1

    if last_dict_data['sec_selling_volume'] < last_dict_data['sec_selling_volume_ema5'] and last_dict_data[
        'sec_selling_volume'] < last_dict_data['sec_selling_volume_ema10']:
        risk_level = "低风险"
    else:
        risk_level = "有风险"
    result_dict.setdefault(risk_level, 0)
    result_dict[risk_level] += 1
    result_dict = {k: round(v / 2, 4) for k, v in result_dict.items()}
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
    return data, last_risk_dict


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
    return data, last_risk_dict


def get_stock_margin_indicator(code):
    stock_margin_daily = get_mongo_table(database='stock', collection='stock_margin_daily')

    start_day = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    datas = []
    for ele in stock_margin_daily.find(
            {"code": code, "time": {"$gt": start_day}},
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


def get_batch_stock_margin_indicator(codes):
    stock_margin_daily = get_mongo_table(database='stock', collection='stock_margin_daily')

    start_day = (datetime.now() - timedelta(days=100)).strftime("%Y%m%d")
    datas = []
    for ele in stock_margin_daily.find(
            {"code": {"$in": codes}, "time": {"$gt": start_day}},
            projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    margin_risk = {}
    for code in codes:
        data = pd_data[pd_data['code'] == code][['sec_selling_volume', 'time', 'fin_purchase_amount']]
        if len(data) > 0:
            data['sec_selling_volume_ema5'] = ta.EMA(data['sec_selling_volume'], timeperiod=5)
            data['sec_selling_volume_ema10'] = ta.EMA(data['sec_selling_volume'], timeperiod=10)

            data['fin_purchase_amount_ema5'] = ta.EMA(data['fin_purchase_amount'], timeperiod=5)
            data['fin_purchase_amount_ema10'] = ta.EMA(data['fin_purchase_amount'], timeperiod=10)
            last_risk_dict = eva_market_margin_risk(dict(data.tail(1).iloc[0]))
            margin_risk[code] = last_risk_dict
    return margin_risk


def get_stock_holder_or_reduce_risk(codes, start_time=None):
    """
    获取最近30日股票是否有股东减持或者增持，起始时间默认前30日
    :param codes:
    :param start_time:
    :return:
    """
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
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
        result_dict_data.setdefault(code, 0)
        result_dict_data[code] += shareholding_change_outstanding_share_rate
    risk_level = {}
    for k, v in result_dict_data.items():
        if v > 0:
            risk_level[k] = {"risk_level": "无风险", "risk_value": 0}
        if v < 0:
            risk_level[k] = {"risk_level": "有风险", "risk_value": 0.6}
    return result_dict_data, risk_level


def get_stock_cyq_risk(codes, start_time=None):
    """筹码上升，风险减少，筹码下降风险增大"""
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=31)).strftime("%Y-%m-%d")
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    cyq_datas = []
    for ele in stock_common.find(
            {"metric_code": {"$in": codes}, "time": {"$gte": start_time}, "data_type": "stock_cyq"},
            projection={'_id': False, 'metric_code': True, 'time': True, 'avg_cost': True}):
        ele['code'] = ele['metric_code']
        cyq_datas.append(ele)
    condition = {"code": {"$in": codes}, "time": {"$gte": start_time}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False, 'time': True, 'code': True, 'close': True}
    sort_key = "time"
    cyq_df = pd.DataFrame(cyq_datas)
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    new_data = pd.merge(cyq_df, data, on=['time', 'code'], how='left')
    avg_cost_data = pd.pivot_table(new_data, values='avg_cost', index='time', columns='code')
    avg_cost_data.sort_index(inplace=True)

    avg_pct = avg_cost_data.pct_change(1)
    avg_pct.dropna(inplace=True)
    count = avg_pct.shape[0]
    avg_cost_risk = dict(avg_pct[avg_pct < 0].count())
    avg_cost_risk = {k: round(v / count, 4) for k, v in avg_cost_risk.items()}
    new_data['up_rate'] = round((new_data['avg_cost'] - new_data['close']) / new_data['close'], 4)
    up_rate = pd.pivot_table(new_data, values='up_rate', index='time', columns='code')
    up_rate.sort_index(inplace=True)
    up_rate_dict = dict(up_rate.tail(1).iloc[0])
    return new_data, up_rate_dict, avg_cost_risk


def get_stock_vol_risk(codes, start_time=None):
    if start_time is None:
        start_time = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    condition = {"code": {"$in": codes}, "time": {"$gte": start_time}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    result_dict = {}
    for code in codes:
        ele_pd_data = data[data['code'] == code]
        ele_pd_data.sort_values(by=sort_key, inplace=True)
        ele_pd_data['atr14'] = ta.ATR(ele_pd_data.high, ele_pd_data.low, ele_pd_data.close, timeperiod=14)
        ele_pd_data['atr14_rate'] = round(ele_pd_data['atr14'] / ele_pd_data['close'], 4)
        std_val = ele_pd_data['close'].pct_change(1).std()
        val_dict_data = {}
        val_dict_data["std"] = round(std_val, 4)
        atr14_rate = ele_pd_data['atr14_rate'].tail(1).values[0]
        val_dict_data['atr14_rate'] = atr14_rate
        result_dict[code] = val_dict_data

    return result_dict

def metric_up_or_dow_risk(metric_data:pd.DataFrame):
    last_dict_data = dict(metric_data.tail(1).iloc[0])
    last_dict_data['index'] = str(metric_data.tail(1).index.values[0])
    result_dict_data = {}
    num = metric_data.shape[0] - 1

    for i,index in enumerate(metric_data.index):
        ele = dict(metric_data.loc[index])
        if index != last_dict_data['index']:
            for k, v in ele.items():
                result_dict_data.setdefault(k, {"up": 0, "down": 0})
                if v < last_dict_data[k]:
                    result_dict_data[k]['up'] += 0.1*(1/num)*pow(0.9,num-i-1)
                if v > last_dict_data[k]:
                    result_dict_data[k]['down'] += 0.1*(1/num)*pow(0.9,num-i-1)
    return result_dict_data


def get_stock_fin_risk(codes, start_time=None,metric_dict_data = None):
    if metric_dict_data is None:
        metric_dict_data = {"lrb":['income_cycle','total_revenue_cycle'],
                            "zcfz":['assets_cycle','lia_assets_cycle'],
                            "xjll":['net_cash_flow_cycle'],
                            }

    if start_time is None:
        start_time = (datetime.now() - timedelta(days=365*10)).strftime("%Y0101")
    condition = {"code": {"$in": codes}, "date": {"$gte": start_time}}
    database = 'stock'
    collection = 'fin_simple'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    result_dict_data = {}
    data = data[data['date'].str.contains('0930')]
    for data_type,metric_cols in metric_dict_data.items():
        for metric_col in metric_cols:
            metric_data = data[data['data_type'] == data_type][['code', metric_col, 'date']]
            metric_data = pd.pivot_table(metric_data, index='date', values=metric_col, columns='code')
            metric_data.sort_index(inplace=True)
            result_dict_data[f'{metric_col}_risk'] = metric_up_or_dow_risk(metric_data)
    return result_dict_data


if __name__ == '__main__':
    pass