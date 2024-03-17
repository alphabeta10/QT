import akshare as ak
from pymongo import UpdateOne
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data
from data.mongodb import get_mongo_table
from datetime import datetime, timedelta
import requests
import pandas as pd


def handle_futures_daily_data(symbols=None):
    if symbols is None:
        symbols = ['B0']
    futures_daily = get_mongo_table(database='futures', collection='futures_daily')
    for symbol in symbols:
        datas = []
        futures_zh_daily_sina_df = try_get_action(ak.futures_zh_daily_sina, try_count=3, symbol=symbol)
        for index in futures_zh_daily_sina_df.index:
            ele_dict = dict(futures_zh_daily_sina_df.loc[index])
            dict_data = {}
            for k, v in ele_dict.items():
                if k == 'date':
                    dict_data[k] = str(v)
                else:
                    dict_data[k] = float(v)
            dict_data['symbol'] = symbol
            datas.append(UpdateOne(
                {"symbol": dict_data['symbol'], "date": dict_data['date']},
                {"$set": dict_data},
                upsert=True))
        if len(datas) > 0:
            mongo_bulk_write_data(futures_daily, datas)


def get_all_inventory_symbols():
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "reportName": "RPT_FUTU_POSITIONCODE",
        "columns": "TRADE_MARKET_CODE,TRADE_CODE,TRADE_TYPE",
        "filter": '(IS_MAINCODE="1")',
        "pageNumber": "1",
        "pageSize": "500",
        "source": "WEB",
        "client": "WEB",
        "_": "1669352163467",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    symbol_dict = dict(zip(temp_df["TRADE_TYPE"], temp_df["TRADE_CODE"]))
    symbols = [k for k in symbol_dict.keys() if
               k not in ['中证1000', '中证500', '沪深300', '上证50', '集运指数(欧线)', '5年期国债', '2年期国债',
                         '10年期国债', '30年期国债期货']]
    return symbols


def handle_futures_inventory_data(symbols=None):
    if symbols is None:
        symbols = get_all_inventory_symbols()
    futures_daily = get_mongo_table(database='futures', collection='futures_inventory')
    for symbol in symbols:
        datas = []
        futures_zh_daily_sina_df = try_get_action(ak.futures_inventory_em, try_count=3, symbol=symbol)
        for index in futures_zh_daily_sina_df.index:
            ele_dict = dict(futures_zh_daily_sina_df.loc[index])
            dict_data = {}
            for k, v in ele_dict.items():
                if k == '日期':
                    dict_data['date'] = str(v)
                else:
                    dict_data[k] = float(v)
            dict_data['symbol'] = symbol
            datas.append(UpdateOne(
                {"symbol": dict_data['symbol'], "date": dict_data['date']},
                {"$set": dict_data},
                upsert=True))
        if len(datas) > 0:
            mongo_bulk_write_data(futures_daily, datas)


def handle_futures_receipt_data(codes=None, start_day=datetime.now().strftime("%Y%m01"),
                                end_day=datetime.now().strftime("%Y%m%d")):
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    if codes is None:
        futures_receipt = try_get_action(ak.get_receipt, try_count=3, start_day=start_day, end_day=end_day)
    else:
        futures_receipt = try_get_action(ak.get_receipt, try_count=3, start_day=start_day, end_day=end_day,
                                         vars_list=codes)
    datas = []
    if futures_receipt is not None:
        for index in futures_receipt.index:
            ele_dict = dict(futures_receipt.loc[index])
            dict_data = {}
            dict_data['code'] = ele_dict['var']
            dict_data['date'] = ele_dict['date']
            dict_data['data_type'] = 'futures_receipt'

            dict_data['value'] = float(ele_dict['receipt'])
            dict_data['value_chg'] = float(ele_dict['receipt_chg'])
            datas.append(UpdateOne(
                {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                {"$set": dict_data},
                upsert=True))
        if len(datas) > 0:
            mongo_bulk_write_data(futures_basic_info, datas)


def handle_futures_czce_warehouse_receipt(trade_dates=None):
    if trade_dates is None:
        trade_dates = ['20240119']
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for trade_date in trade_dates:
        print(f"handel 郑商所仓单日报数据日期 {trade_date}")
        czce_warehouse_receipt_df = try_get_action(ak.futures_czce_warehouse_receipt, try_count=3,
                                                   trade_date=trade_date)
        if czce_warehouse_receipt_df is not None:
            for k, v in czce_warehouse_receipt_df.items():
                columns = v.columns
                data = None
                if '仓库编号' in columns:
                    data = v[v['仓库编号'] == '总计']
                if '机构编号' in columns:
                    data = v[v['机构编号'] == '总计']
                if '厂库编号' in columns:
                    data = v[v['厂库编号'] == '总计']
                if data is not None and len(data) > 0:
                    ele_dict_data = dict(data.iloc[0])
                    dict_data = {"code": k, "data_type": "futures_warehouse_receipt"}
                    wh_count = 0
                    for ck, v in ele_dict_data.items():
                        if '仓单数量' in ck and abs(v) > 0:
                            wh_count += v
                    dict_data['value_chg'] = float(ele_dict_data['当日增减'])
                    dict_data['value'] = float(wh_count)
                    dict_data['date'] = trade_date
                    datas.append(UpdateOne(
                        {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                        {"$set": dict_data},
                        upsert=True))
                else:
                    print(f"no_data check {k}")
                    print(v)
            if len(datas) > 50:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()
    if len(datas) > 0:
        mongo_bulk_write_data(futures_basic_info, datas)


def handle_futures_dce_warehouse_receipt(trade_dates=None):
    if trade_dates is None:
        trade_dates = ['20240119']
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for trade_date in trade_dates:
        print(f"handel 大商所仓单日报数据日期 {trade_date}")
        futures_dce_warehouse_receipt_df = try_get_action(ak.futures_dce_warehouse_receipt, try_count=3,
                                                          trade_date=trade_date)
        if futures_dce_warehouse_receipt_df is not None:
            for k, v in futures_dce_warehouse_receipt_df.items():
                code = f"{k}小计"
                data = v[v['品种'] == code]
                ele_dict_data = dict(data.iloc[0])
                dict_data = {"code": k, "data_type": "futures_warehouse_receipt"}
                dict_data['value_chg'] = float(ele_dict_data['增减'])
                dict_data['value'] = float(ele_dict_data['今日仓单量'])
                dict_data['date'] = trade_date
                datas.append(UpdateOne(
                    {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                    {"$set": dict_data},
                    upsert=True))
            if len(datas) > 50:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()
    if len(datas) > 0:
        mongo_bulk_write_data(futures_basic_info, datas)


def handle_futures_shfe_warehouse_receipt(trade_dates=None):
    if trade_dates is None:
        trade_dates = ['20240119']
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for trade_date in trade_dates:
        print(f"handel 上期所仓单日报数据日期 {trade_date}")
        futures_shfe_warehouse_receipt = try_get_action(ak.futures_shfe_warehouse_receipt, try_count=3,
                                                        trade_date=trade_date)
        if futures_shfe_warehouse_receipt is not None:
            for k, v in futures_shfe_warehouse_receipt.items():
                data = v[v['WHABBRNAME'] == '总计']
                dict_data = {"code": k, "data_type": "futures_warehouse_receipt"}
                if k == '黄金':
                    data = v.head(1)
                elif len(data) == 0:
                    data = v[v['WHABBRNAME'] == '合计']
                if data is not None and len(data) > 0:
                    ele_dict_data = dict(data.iloc[0])
                    dict_data['value'] = float(ele_dict_data['WRTWGHTS'])
                    dict_data['value_chg'] = float(ele_dict_data['WRTCHANGE'])
                else:
                    print(f"no data check {k}")
                    print(v)
                dict_data['date'] = trade_date
                datas.append(UpdateOne(
                    {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                    {"$set": dict_data},
                    upsert=True))
            if len(datas) > 50:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()
    if len(datas) > 0:
        mongo_bulk_write_data(futures_basic_info, datas)


def handle_futures_delivery_dce(dates=None):
    if dates is None:
        dates = ['202401']
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for month in dates:
        print(f"handel 大商所交割数据月日期{month}")
        futures_delivery_dce_df = try_get_action(ak.futures_delivery_dce, try_count=3,
                                                 date=month)
        if futures_delivery_dce_df is not None:
            for index in futures_delivery_dce_df.index:
                ele_dict_data = dict(futures_delivery_dce_df.loc[index])
                code = ele_dict_data['品种'].replace(" ", "")
                date = str(ele_dict_data['交割日期'])
                if date != 'nan':
                    dict_data = {"code": code, "data_type": "futures_delivery"}
                    dict_data['delivery_amount'] = float(ele_dict_data['交割金额'])
                    dict_data['delivery_volume'] = float(ele_dict_data['交割量'])
                    dict_data['date'] = date
                    datas.append(UpdateOne(
                        {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                        {"$set": dict_data},
                        upsert=True))
            if len(datas) > 50:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()
    if len(datas) > 0:
        mongo_bulk_write_data(futures_basic_info, datas)


def handel_futures_long_short_data_cffex(dates: list, codes: list):
    """
    金融板块期货多空信息数据
    :return:
    """
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for date in dates:
        print(f"handle date={date}")
        data = try_get_action(ak.get_cffex_rank_table, try_count=3, date=date, vars_list=codes)
        if data is not None:
            for k, v in data.items():
                sum_long_open_interest = v['long_open_interest'].sum()
                sum_long_open_interest_chg = v['long_open_interest_chg'].sum()
                sum_short_open_interest = v['short_open_interest'].sum()
                sum_short_open_interest_chg = v['short_open_interest_chg'].sum()

                long_short_rate = round(sum_long_open_interest / sum_short_open_interest, 4)
                position_result = {}
                position_result['code'] = k
                position_result['date'] = date
                position_result['long_open_interest'] = int(sum_long_open_interest)
                position_result['long_open_interest_chg'] = int(sum_long_open_interest_chg)

                position_result['short_open_interest'] = int(sum_short_open_interest)
                position_result['short_open_interest_chg'] = int(sum_short_open_interest_chg)
                position_result['long_short_rate'] = long_short_rate
                position_result['data_type'] = "futures_long_short_rate"
                datas.append(UpdateOne(
                    {"code": position_result['code'], "data_type": position_result['data_type'],
                     "date": position_result['date']},
                    {"$set": position_result},
                    upsert=True))
            if len(datas) > 0:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()


def handel_futures_long_short_data_dce(dates: list, codes: list):
    """
    大连期货多空信息数据
    :return:
    """
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for date in dates:
        print(f"handle date={date}")
        data = try_get_action(ak.futures_dce_position_rank, try_count=3, date=date, vars_list=codes)
        if data is not None:
            for k, v in data.items():
                sum_long_open_interest = v['long_open_interest'].sum()
                sum_long_open_interest_chg = v['long_open_interest_chg'].sum()
                sum_short_open_interest = v['short_open_interest'].sum()
                sum_short_open_interest_chg = v['short_open_interest_chg'].sum()

                long_short_rate = round(sum_long_open_interest / sum_short_open_interest, 4)
                position_result = {}
                position_result['code'] = k
                position_result['date'] = date
                position_result['long_open_interest'] = int(sum_long_open_interest)
                position_result['long_open_interest_chg'] = int(sum_long_open_interest_chg)

                position_result['short_open_interest'] = int(sum_short_open_interest)
                position_result['short_open_interest_chg'] = int(sum_short_open_interest_chg)
                position_result['long_short_rate'] = long_short_rate
                position_result['data_type'] = "futures_long_short_rate"
                datas.append(UpdateOne(
                    {"code": position_result['code'], "data_type": position_result['data_type'],
                     "date": position_result['date']},
                    {"$set": position_result},
                    upsert=True))
            if len(datas) > 0:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()


def handel_futures_long_short_data_shfe(dates: list, codes: list):
    """
    上期货多空信息数据
    :return:
    """
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for date in dates:
        print(f"handle date={date}")
        data = try_get_action(ak.get_shfe_rank_table, try_count=3, date=date, vars_list=codes)
        if data is not None:
            for k, v in data.items():
                sum_long_open_interest = v['long_open_interest'].sum()
                sum_long_open_interest_chg = v['long_open_interest_chg'].sum()
                sum_short_open_interest = v['short_open_interest'].sum()
                sum_short_open_interest_chg = v['short_open_interest_chg'].sum()

                long_short_rate = round(sum_long_open_interest / sum_short_open_interest, 4)
                position_result = {}
                position_result['code'] = k
                position_result['date'] = date
                position_result['long_open_interest'] = int(sum_long_open_interest)
                position_result['long_open_interest_chg'] = int(sum_long_open_interest_chg)

                position_result['short_open_interest'] = int(sum_short_open_interest)
                position_result['short_open_interest_chg'] = int(sum_short_open_interest_chg)
                position_result['long_short_rate'] = long_short_rate
                position_result['data_type'] = "futures_long_short_rate"
                datas.append(UpdateOne(
                    {"code": position_result['code'], "data_type": position_result['data_type'],
                     "date": position_result['date']},
                    {"$set": position_result},
                    upsert=True))
            if len(datas) > 0:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()


def handel_futures_long_short_data_gfex(dates: list, codes: list):
    """
    广期货多空信息数据
    :return:
    """
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for date in dates:
        print(f"handle date={date}")
        data = try_get_action(ak.futures_gfex_position_rank, try_count=3, date=date, vars_list=codes)
        if data is not None:
            for k, v in data.items():
                v['long_open_interest'] = v.apply(lambda ele: int(str(ele['long_open_interest']).replace(",", "")),
                                                  axis=1)
                v['long_open_interest_chg'] = v.apply(
                    lambda ele: int(str(ele['long_open_interest_chg']).replace(",", "")), axis=1)
                v['short_open_interest'] = v.apply(lambda ele: int(str(ele['short_open_interest']).replace(",", "")),
                                                   axis=1)
                v['short_open_interest_chg'] = v.apply(
                    lambda ele: int(str(ele['short_open_interest_chg']).replace(",", "")), axis=1)

                sum_long_open_interest = v['long_open_interest'].sum()
                sum_long_open_interest_chg = v['long_open_interest_chg'].sum()
                sum_short_open_interest = v['short_open_interest'].sum()
                sum_short_open_interest_chg = v['short_open_interest_chg'].sum()

                long_short_rate = round(sum_long_open_interest / sum_short_open_interest, 4)
                position_result = {}
                position_result['code'] = k
                position_result['date'] = date
                position_result['long_open_interest'] = int(sum_long_open_interest)
                position_result['long_open_interest_chg'] = int(sum_long_open_interest_chg)

                position_result['short_open_interest'] = int(sum_short_open_interest)
                position_result['short_open_interest_chg'] = int(sum_short_open_interest_chg)
                position_result['long_short_rate'] = long_short_rate
                position_result['data_type'] = "futures_long_short_rate"
                datas.append(UpdateOne(
                    {"code": position_result['code'], "data_type": position_result['data_type'],
                     "date": position_result['date']},
                    {"$set": position_result},
                    upsert=True))
            if len(datas) > 0:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()


def handle_futures_delivery_czce(trade_dates=None):
    if trade_dates is None:
        trade_dates = ['20240119']
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for trade_date in trade_dates:
        print(f"handel 郑商所交割数据日期{trade_date}")
        futures_delivery_dce_df = try_get_action(ak.futures_delivery_czce, try_count=3,
                                                 date=trade_date)
        if futures_delivery_dce_df is not None:
            for index in futures_delivery_dce_df.index:
                ele_dict_data = dict(futures_delivery_dce_df.loc[index])
                code = ele_dict_data['品种']
                if code != '合计':
                    dict_data = {"code": code, "data_type": "futures_delivery"}
                    dict_data['delivery_amount'] = float(ele_dict_data['交割额'])
                    dict_data['delivery_volume'] = float(ele_dict_data['交割数量'])
                    dict_data['date'] = trade_date
                    datas.append(UpdateOne(
                        {"code": dict_data['code'], "data_type": dict_data['data_type'], "date": dict_data['date']},
                        {"$set": dict_data},
                        upsert=True))
            if len(datas) > 50:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()
    if len(datas) > 0:
        mongo_bulk_write_data(futures_basic_info, datas)


def handel_futures_long_short_data_czce(dates: list, codes: list):
    """
    郑期货多空信息数据
    :return:
    """
    datas = []
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    for date in dates:
        print(f"handle date={date}")
        data = try_get_action(ak.get_czce_rank_table, try_count=3, date=date, vars_list=codes)
        if data is not None:
            for k, v in data.items():
                v['long_open_interest'] = v.apply(
                    lambda ele: int(str(ele['long_open_interest']).replace(",", "")) if str(
                        ele['long_open_interest']) != '-' else 0, axis=1)
                v['long_open_interest_chg'] = v.apply(
                    lambda ele: int(str(ele['long_open_interest_chg']).replace(",", "")) if str(
                        ele['long_open_interest_chg']) != '-' else 0, axis=1)
                v['short_open_interest'] = v.apply(
                    lambda ele: int(str(ele['short_open_interest']).replace(",", "")) if str(
                        ele['short_open_interest']) != '-' else 0, axis=1)
                v['short_open_interest_chg'] = v.apply(
                    lambda ele: int(str(ele['short_open_interest_chg']).replace(",", "")) if str(
                        ele['short_open_interest_chg']) != '-' else 0, axis=1)

                sum_long_open_interest = v['long_open_interest'].sum()
                sum_long_open_interest_chg = v['long_open_interest_chg'].sum()
                sum_short_open_interest = v['short_open_interest'].sum()
                sum_short_open_interest_chg = v['short_open_interest_chg'].sum()

                long_short_rate = round(sum_long_open_interest / sum_short_open_interest, 4)
                position_result = {}
                position_result['code'] = k
                position_result['date'] = date
                position_result['long_open_interest'] = int(sum_long_open_interest)
                position_result['long_open_interest_chg'] = int(sum_long_open_interest_chg)

                position_result['short_open_interest'] = int(sum_short_open_interest)
                position_result['short_open_interest_chg'] = int(sum_short_open_interest_chg)
                position_result['long_short_rate'] = long_short_rate
                position_result['data_type'] = "futures_long_short_rate"
                datas.append(UpdateOne(
                    {"code": position_result['code'], "data_type": position_result['data_type'],
                     "date": position_result['date']},
                    {"$set": position_result},
                    upsert=True))
            if len(datas) > 0:
                mongo_bulk_write_data(futures_basic_info, datas)
                datas.clear()


def futures_long_short_rate_codes():
    futures_rule_df = ak.futures_rule(date="20240315")
    dict_codes = {}
    for index in futures_rule_df.index:
        dict_data = dict(futures_rule_df.loc[index])
        symbol = dict_data['品种']
        code = dict_data['代码']
        trade_futures = dict_data['交易所']
        if '期权' not in symbol:
            dict_codes.setdefault(trade_futures, [])
            dict_codes[trade_futures].append({"symbol": symbol, "code": code})
    return dict_codes


def enter_futrures_long_short_main(before_days=5):
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    trade_dates = []
    month_dates = set()
    now_int = int(datetime.now().strftime("%Y%m%d"))
    before_day_int = int((datetime.now() - timedelta(days=before_days)).strftime("%Y%m%d"))
    for index in tool_trade_date_hist_sina_df.index:
        trade_date = tool_trade_date_hist_sina_df.loc[index]['trade_date']
        date_str = str(trade_date).replace("-", "")
        if int(date_str) > before_day_int and int(date_str) <= now_int:
            trade_dates.append(date_str)
            month_dates.add(date_str[:6])

    dict_codes = futures_long_short_rate_codes()
    fn_mapping = {
        "大商所": handel_futures_long_short_data_dce,
        "郑商所": handel_futures_long_short_data_czce,
        "上期所": handel_futures_long_short_data_shfe,
        "广期所": handel_futures_long_short_data_gfex,
        "中金所": handel_futures_long_short_data_cffex,
    }
    for k, fn in fn_mapping.items():
        print(f"handle {k}")
        codes = [ele['code'] for ele in dict_codes[k] if '期权' not in ele['symbol']]
        fn(trade_dates, codes)


def col_create_index():
    futures_daily = get_mongo_table(database='futures', collection='futures_daily')
    futures_daily.create_index([("symbol", 1), ("date", 1)], unique=True, background=True)


def create_futures_inventory_index():
    futures_inventory = get_mongo_table(database='futures', collection='futures_inventory')
    futures_inventory.create_index([("symbol", 1), ("date", 1)], unique=True, background=True)


def create_futures_basic_info_index():
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    futures_basic_info.create_index([("code", 1), ('data_type', 1), ("date", 1)], unique=True, background=True)


if __name__ == '__main__':
    handle_futures_daily_data()
