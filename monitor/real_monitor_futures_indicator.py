import time

from analysis.market_analysis import *
from utils.actions import show_data
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal
pd.set_option( 'display.precision',4)




def real_monitor_futures_and_cal_indicator():
    code_name = 'symbol'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    code_config_list = [
        {"name": "玻璃连续", "market_type": "CF", "code": "FG0","stop_price":1000,"stop_time":1,"contract_chg_price":20},
        {"name": "豆二连续", "market_type": "CF", "code": "B0","stop_price":1000,"stop_time":1,"contract_chg_price":10},
        {"name": "螺纹钢连续", "market_type": "CF", "code": "RB0","stop_price":1000,"stop_time":1,"contract_chg_price":10},
        {"name": "乙二醇连续", "market_type": "CF", "code": "EG0","stop_price":1000,"stop_time":1,"contract_chg_price":10},
    ]
    codes = [ele['code'] for ele in code_config_list]
    condition = {code_name: {"$in": codes}, "date": {"$gte":start_date}}
    database = 'futures'
    collection = 'futures_daily'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'symbol']
    data.index = pd.to_datetime(data[sort_key])
    data = data[cols]
    cf_code = ",".join([ele['code'] for ele in code_config_list if ele['market_type'] == 'CF'])
    ff_code = ",".join([ele['code'] for ele in code_config_list if ele['market_type'] == 'FF'])
    print(f"cf code {cf_code}")
    print(f"ff code {ff_code}")
    r_col = ['high', 'current_price', 'low', 'volume', 'symbol']

    while True:
        cf_futures_zh_spot_df = None
        ff_futures_zh_spot_df = None
        if cf_code!='':
            cf_futures_zh_spot_df = ak.futures_zh_spot(symbol=cf_code, market="CF", adjust='0')
            cf_futures_zh_spot_df['date'] = datetime.now().strftime("%Y-%m-%d")
            cf_futures_zh_spot_df.index = pd.to_datetime(cf_futures_zh_spot_df['date'])
            cf_futures_zh_spot_df = cf_futures_zh_spot_df[r_col]
        if ff_code!='':
            ff_futures_zh_spot_df = ak.futures_zh_spot(symbol=ff_code, market="FF", adjust='0')
            ff_futures_zh_spot_df['date'] = datetime.now().strftime("%Y-%m-%d")
            ff_futures_zh_spot_df.index = pd.to_datetime(ff_futures_zh_spot_df['date'])
            ff_futures_zh_spot_df = ff_futures_zh_spot_df[r_col]

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        for futures_cf in code_config_list:
            name = futures_cf['name']
            code = futures_cf['code']
            market_type = futures_cf['market_type']
            pd_data = data[data[code_name] == code][cols]
            new_data = None
            if cf_futures_zh_spot_df is not None and market_type=='CF':
                new_data = cf_futures_zh_spot_df[cf_futures_zh_spot_df['symbol']==name]
                new_data['symbol'] = code
                new_data.rename(columns={"current_price":"close"},inplace=True)
            if ff_futures_zh_spot_df is not None and market_type=='FF':
                new_data = ff_futures_zh_spot_df[ff_futures_zh_spot_df['symbol']==name]
                new_data['symbol'] = code
                new_data.rename(columns={"current_price":"close"},inplace=True)
            if new_data is not None:
                new_data = pd.concat([pd_data, new_data])
                common_indictator_cal(new_data, ma_timeperiod=20)
                new_data['name'] = name
                new_data['position'] = futures_cf['stop_price']/(new_data['atr14']*futures_cf['stop_time']*futures_cf['contract_chg_price'])
                show_data(new_data.tail(1))
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        time.sleep(80)

def get_futures_name():
    cf_futures_zh_spot_df = ak.futures_zh_spot(symbol='B0,FG0', market="CF", adjust='0')
    show_data(cf_futures_zh_spot_df)

if __name__ == '__main__':
    real_monitor_futures_and_cal_indicator()
