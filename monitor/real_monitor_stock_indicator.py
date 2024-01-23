import time

from analysis.market_analysis import *
from utils.actions import show_data
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal


def real_monitor_stock_and_cal_indicator():
    code_name = 'code'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    code_dict = {
        # 半导体
        "002409": "雅克科技",
        # 电力
        "002015": "协鑫能科",
        # 游戏
        "002555": "三七互娱",
        "002602": "世纪华通",
        "603444": "吉比特",
        # 通讯
        "000063": "中兴通讯",
        "600522": "中天科技",
        # 白酒
        "000858": "五粮液",
        "600519": "贵州茅台",
        # 机器人
        "002472": "双环传动",
        # 银行
        "600036": "招商银行",
        "600919": "江苏银行",
        # AI相关
        "300474": "景嘉微",
        "002230": "科大讯飞",
        "603019": "中科曙光",
        "000977": "浪潮信息",
        # 新能源
        "300750": "宁德时代",
        "002594": "比亚迪",
        # 零食
        "300783": "三只松鼠",
        "603719": "良品铺子",
        # 啤酒
        "600132": "重庆啤酒",
        "600600": "青岛啤酒",

    }
    codes = list(code_dict.keys())
    condition = {code_name: {"$in": codes}, "time": {"$gte": start_date}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'code']
    data.index = pd.to_datetime(data[sort_key])
    while True:
        stock_zh_a_spot_em_df = try_get_action(ak.stock_zh_a_spot_em, try_count=3)
        c_cols = {'代码': "code", '成交量': "volume", '最高': "high", '最低': "low", '最新价': "close"}
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['代码'].isin(codes)][list(c_cols.keys())]
        stock_zh_a_spot_em_df.rename(columns=c_cols, inplace=True)
        stock_zh_a_spot_em_df['time'] = datetime.now().strftime("%Y-%m-%d")
        stock_zh_a_spot_em_df.index = pd.to_datetime(stock_zh_a_spot_em_df['time'])
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[cols]
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        for code in codes:
            pd_data = data[data[code_name] == code][cols]
            today_data = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df[code_name] == code]
            new_data = pd.concat([pd_data, today_data])
            new_data['name'] = code_dict.get(code)
            common_indictator_cal(new_data, ma_timeperiod=20)
            new_data['stop_rate'] = round((new_data['atr14']*3)/new_data['close'],4)
            show_data(new_data.tail(1))
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        time.sleep(80)


if __name__ == '__main__':
    real_monitor_stock_and_cal_indicator()
