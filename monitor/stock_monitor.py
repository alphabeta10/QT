import akshare as ak
from utils.actions import try_get_action
from utils.tool import get_mongo_table
from datetime import datetime, timedelta


def tele_news_monitor(monitor_keys=None):
    if monitor_keys is None:
        monitor_keys = ['科大讯飞', '人工智能', '白酒', '雅克科技', '景嘉微', '江苏银行', '玻璃']
    news = get_mongo_table(database='stock', collection='news')
    date_time = datetime.now() - timedelta(days=0)
    today = date_time.strftime("%Y-%m-%d")
    dict_key_words = {ele: {} for ele in monitor_keys}
    for ele in news.find({"data_type": "cls_telegraph", "time": {"$gt": f"{today}"}}, projection={'_id': False}).sort(
            "time"):
        content = ele['content']
        title = ele['title']
        for key in monitor_keys:
            if key in content:
                dict_key_words[key][title] = content + " time=" + ele['time']
    for k, v in dict_key_words.items():
        print(50 * "*" + f"{k}" + 50 * "*")
        for t, c in v.items():
            print("-" * 10)
            print(t, c)
            print("-" * 10)
        print(50 * "*" + f"{k}" + 50 * "*")


if __name__ == '__main__':
    pass
