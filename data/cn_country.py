import requests
from bs4 import BeautifulSoup
import re
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
import jieba
import jieba.posseg as psseg
from datetime import datetime

def get_country_data(url='https://www.fmprc.gov.cn/zyxw/index.shtml'):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    datas = []
    div_news_bds = soup.find_all("div","newsBd")
    if div_news_bds is not None and len(div_news_bds)>0:
        div_news_bd = div_news_bds[0]
        a_s = div_news_bd.find_all("a")
        if a_s is not None and len(a_s)>0:
            for a in a_s:
                text = a.text
                day = re.findall("（(\d+)-(\d+)-(\d+)）", text)
                if len(day)>0:
                    day = day[0]
                    day = f"{day[0]}-{day[1]}-{day[2]}"
                    text = text.replace(f"（{day}）","")
                    day = day.replace("-","")
                    dict_data = {"time":day,"data_type":"rel_country","title":text}
                    datas.append(UpdateOne(
                        {"data_type": dict_data['data_type'],"title": dict_data['title']},
                        {"$set": dict_data},
                        upsert=True))
                else:
                    print(f"error {text}")
    return datas


def handle_rl_country():
    news = get_mongo_table(database='stock', collection='news')
    for i in range(0,3):
        if i >0:
            url = f'https://www.fmprc.gov.cn/zyxw/index_{i}.shtml'
        else:
            url = 'https://www.fmprc.gov.cn/zyxw/index.shtml'
        datas = get_country_data(url=url)
        if len(datas)>0:
            mongo_bulk_write_data(news, datas)
            print(url)

def find_data():
    news = get_mongo_table(database='stock', collection='news')
    datas = []
    dict_country = {}
    today = datetime.now().strftime("%Y0101")
    for ele in news.find({"data_type": "rel_country","time":{"$gt":f"{today}"}}, projection={'_id': False}).sort("time"):
        datas.append(ele)
        title = ele['title']
        print(ele)
        ts = jieba.cut(title,use_paddle=True)
        print("/".join(ts),ele['time'])
        words = psseg.cut(title)
        for word,flag in words:
            print(word,flag)
            if flag=='ns':
                if word not in dict_country.keys():
                    dict_country[word] = 0
                dict_country[word] += 1
    print(sorted(dict_country.items(),key=lambda x:(x[1],x[0]),reverse=True))




if __name__ == '__main__':
    handle_rl_country()
    find_data()