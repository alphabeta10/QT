import time

from analysis.market_analysis import *
from utils.actions import show_data
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal


def real_monitor_stock_index_and_cal_indicator():
    code_name = 'code'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    code_dict = {
        "sh000001": "上证指数",
        "sz399001":"深证成指"
    }
    codes = list(code_dict.keys())
    condition = {code_name: {"$in": codes}, "date": {"$gte": start_date}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'code']
    data.index = pd.to_datetime(data[sort_key])
    while True:
        stock_index_zh_a_spot_em_df = try_get_action(ak.stock_zh_index_spot_sina, try_count=3)
        c_cols = {'代码': "code", '成交量': "volume", '最高': "high", '最低': "low", '最新价': "close"}
        stock_index_zh_a_spot_em_df = stock_index_zh_a_spot_em_df[stock_index_zh_a_spot_em_df['代码'].isin(codes)][list(c_cols.keys())]
        stock_index_zh_a_spot_em_df.rename(columns=c_cols, inplace=True)
        stock_index_zh_a_spot_em_df['date'] = datetime.now().strftime("%Y-%m-%d")
        stock_index_zh_a_spot_em_df.index = pd.to_datetime(stock_index_zh_a_spot_em_df['date'])
        stock_index_zh_a_spot_em_df = stock_index_zh_a_spot_em_df[cols]
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        for code in codes:
            pd_data = data[data[code_name] == code][cols]
            today_data = stock_index_zh_a_spot_em_df[stock_index_zh_a_spot_em_df[code_name] == code]
            new_data = pd.concat([pd_data, today_data])
            new_data['name'] = code_dict.get(code)
            common_indictator_cal(new_data, ma_timeperiod=20)
            new_data['stop_rate'] = round((new_data['atr14']*3)/new_data['close'],4)
            show_data(new_data.tail(1))
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        time.sleep(80)


if __name__ == '__main__':
    real_monitor_stock_index_and_cal_indicator()
