from data.mongodb import get_mongo_table


def get_model_ai_new_indicator_from_db(codes, start_time):
    model_indicator_col = get_mongo_table(database='stock', collection="model_new_indicator")
    sentiment_dict = {}
    for ele in model_indicator_col.find(
            {"data_type": "big_model_sentiment", "time": {"$gt": start_time}, "code": {"$in": codes}},
            projection={'_id': False}).sort(
        "time"):
        sentiment = ele['sentiment']
        code = ele['code']
        sentiment_dict.setdefault(code, {sentiment: 0})
        if sentiment not in sentiment_dict[code].keys():
            sentiment_dict[code][sentiment] = 0
        sentiment_dict[code][sentiment] += 1
    return sentiment_dict
def get_model_stock_indicator_from_db(codes,start_time):
    model_indicator_col = get_mongo_table(database='stock', collection="big_model")
    sentiment_dict = {}
    for ele in model_indicator_col.find(
            {"data_type": "news", "time": {"$gt": start_time}, "code": {"$in": codes}},
            projection={'_id': False}).sort(
        "time"):
        sentiment = ele['sentiment']
        code = ele['code']
        sentiment_dict.setdefault(code, {sentiment: 0})
        if sentiment not in sentiment_dict[code].keys():
            sentiment_dict[code][sentiment] = 0
        sentiment_dict[code][sentiment] += 1
    return sentiment_dict

if __name__ == '__main__':
    pass