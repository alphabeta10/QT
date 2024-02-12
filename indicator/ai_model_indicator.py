from data.mongodb import get_mongo_table
from datetime import datetime,timedelta

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

def get_macro_indicator_from_db():
    model_indicator_col = get_mongo_table(database='stock', collection="big_model")
    last_sentiment = None
    month = (datetime.now()-timedelta(days=60)).strftime("%Y%m01")
    for ele in model_indicator_col.find(
            {"data_type": "macro_summary", "time": {"$gt": month}, "code":"big_model_summary_macro"},
            projection={'_id': False}).sort(
        "time"):
        sentiment = ele['sentiment']
        last_sentiment = sentiment
    return last_sentiment

def get_stock_price_summary_from_db(codes):
    model_indicator_col = get_mongo_table(database='stock', collection="big_model")
    last_abstract_dict = {}
    year = datetime.now().strftime("%Y-01-01")
    for ele in model_indicator_col.find(
            {"data_type": "stock_price_summary", "time": {"$gte": year}, "code":{"$in":codes}},
            projection={'_id': False}).sort(
        "time"):
        abstract = ele['abstract']
        code = ele['code']
        invent_state = ele['invent_state']
        risk_value = 0.3 if invent_state==1 else 0.6
        last_abstract_dict[code] = {"summary":abstract,"risk_value":risk_value}
    return last_abstract_dict

if __name__ == '__main__':
    pass