import akshare as ak
import pandas as pd
from datetime import datetime
from ...live.models import TickData
from utils.actions import try_get_action

def get_stock_real_data(symbol, token=None, real_type='xq'):

    if real_type=='xq':
        stock_individual_spot_xq_df = try_get_action(ak.stock_individual_spot_xq,try_count=3,symbol=symbol.replace(".", "").upper(), token=token)
        if stock_individual_spot_xq_df is not None:
            last_dict_data = {}
            for index in stock_individual_spot_xq_df.index:
                dict_data = dict(stock_individual_spot_xq_df.iloc[index])
                last_dict_data[dict_data.get("item")] = dict_data.get('value')
            if len(last_dict_data.keys()) == 0:
                return None
            tick = TickData(
                symbol=symbol,
                price=float(last_dict_data.get("现价")),
                timestamp=datetime.strptime(last_dict_data.get('时间'), '%Y-%m-%d %H:%M:%S'),
                volume=int(last_dict_data.get('成交量')),
                source="baostock_warmup",
                open=last_dict_data.get("今开"),
                low=last_dict_data.get("最低"),
                high=last_dict_data.get("最高"),
            )
            return tick
    return None

def get_future_real_data(cf_symbol_str,ff_symbol_str=None,**kwargs):
    ff_df = try_get_action(ak.futures_zh_spot,try_count=3,symbol=ff_symbol_str, market="FF", adjust='0') if ff_symbol_str is not None else pd.DataFrame()
    cf_df = try_get_action(ak.futures_zh_spot,try_count=3,symbol=cf_symbol_str, market="CF", adjust='0') if cf_symbol_str is not None else pd.DataFrame()
    if cf_df is not None and cf_df is not None:
        return pd.concat([ff_df,cf_df])
    if cf_df is not None:
        return cf_df
    if ff_df is not None:
        return ff_df




if __name__ == '__main__':
    df = get_future_real_data("B0,FG0")
    print(df)


