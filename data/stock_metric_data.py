import akshare as ak
from tqdm import tqdm
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data



def stock_indicator(codes=None):
    if codes is None:
        stock_name = try_get_action(ak.stock_a_indicator_lg,3,symbol="all")
        codes = stock_name['code'].values
    stock_common = get_mongo_table(collection='stock_common')
    request_update = []
    for code in tqdm(codes):
        try:
            stock_a_indicator_df = try_get_action(ak.stock_a_indicator_lg,3,symbol=code)
            if stock_a_indicator_df is not None and stock_a_indicator_df.empty is False:
                stock_a_indicator_df.fillna(-1,inplace=True)
                stock_a_indicator_df.sort_values(by='trade_date', inplace=True, ascending=False)
                stock_a_indicator_df = stock_a_indicator_df.head(1)
                for index in stock_a_indicator_df.index:
                    dict_data = dict(stock_a_indicator_df.loc[index])
                    dict_data['metric_code'] = code
                    dict_data['data_type'] = "stock_indicator"
                    dict_data['trade_date'] = str(dict_data.get("trade_date"))[0:10]
                    request_update.append(UpdateOne(
                        {"metric_code": dict_data['metric_code'], "data_type": dict_data['data_type']},
                        {"$set": dict_data},
                        upsert=True))
            if len(request_update)>100:
                mongo_bulk_write_data(stock_common,request_update)
                request_update.clear()
        except Exception as e:
            print(e,code)
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_common, request_update)
        request_update.clear()



def stock_vol_and_name(codes = None):
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    if codes is None:
        tickers = ['002162']
    else:
        tickers = codes
    request_update = []
    stock_common = get_mongo_table(collection='stock_common')
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['代码'].isin(tickers)]
    for index in stock_zh_a_spot_em_df.index:
        dict_data = dict(stock_zh_a_spot_em_df.loc[index])
        name = dict_data['名称']
        dict_data['metric_code'] = dict_data['代码']
        dict_data['data_type'] = 'stock_indicator'

        total_vol = dict_data['总市值']/dict_data['最新价']
        new_dict = {"name":name,"total_vol":total_vol,"metric_code": dict_data['代码'],"data_type":'stock_indicator',"total_mv":float(dict_data['总市值'])}
        request_update.append(UpdateOne(
                        {"metric_code": new_dict['metric_code'], "data_type": new_dict['data_type']},
                        {"$set": new_dict},
                        upsert=True))
        if len(request_update) > 100:
            mongo_bulk_write_data(stock_common, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_common, request_update)
        request_update.clear()


if __name__ == '__main__':
    stock_indicator()
    stock_vol_and_name()
