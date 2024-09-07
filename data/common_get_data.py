from data.mongodb import get_mongo_table


def get_ticker_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False})
    ret_data = []
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        code, lr = ts_code.split(".")
        ticker['code'] = code
        ticker['market'] = lr
        ret_data.append(ticker)
    return ret_data

if __name__ == '__main__':
    ret_data = get_ticker_info_data()
    print(ret_data)
