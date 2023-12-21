import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import re
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data


def get_li_price_data(name,url='https://www.100ppi.com/vane/detail-733.html'):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div",'list-event')

    headers = search_div[0]
    lis = headers.find_all('li')
    datas = []
    for li in lis:
        a_s = li.find_all("a")
        if len(a_s)==2:
            span = li.find_all("a")[0]
            data_type = span.text.replace('\n', "").replace(' ', '')
            a = li.find_all('a')[1]
        else:
            data_type = "default"
            a = li.find_all('a')[0]
        header_text = a.text.replace('\n',"").replace(' ','')
        time_span = li.find_all("span")[0]
        time_text = time_span.text.replace('\n',"")
        dict_data = {"data_type":"goods_news","time":time_text,"day":time_text[0:10],"header":header_text,"header_type":data_type,"name":name}
        datas.append(UpdateOne(
            {"header": dict_data['header'], "header_type": dict_data['header_type'], "data_type": dict_data['data_type'],"name":dict_data['name']},
            {"$set": dict_data},
            upsert=True))
    return datas




def get_all_detail_data():
    url = 'http://www.100ppi.com/monitor/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div", 'right fl')
    news = get_mongo_table(database='stock', collection='news')

    headers = search_div[0]
    header_url = "http://www.100ppi.com"
    tables = headers.find_all('table')
    for table in tables:
        trs = table.find_all('tr')
        for tr in trs[1:]:
            urls = tr.find_all("a")
            href = urls[0].get("href")

            if ".html" in href:
                url = f"{header_url}{href}"
                name = urls[0].text.replace(' ', '').replace("\n", '')
                try:
                    x = get_li_price_data(name,url)
                    if len(x)>0:
                        mongo_bulk_write_data(news,x)
                except Exception as e:
                    print(e)
                    print(url)




def get_all_monitor_price_data():
    url = 'http://www.100ppi.com/monitor/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div", 'right fl')
    headers = search_div[0]
    tables = headers.find_all('table')
    for table in tables:
        trs = table.find_all('tr')
        headtds = trs[0].find_all('td')
        date1 = headtds[2].text.replace(' ','')
        date2 = headtds[3].text.replace(' ','')
        date3 = headtds[4].text.replace(' ','')
        datas = []
        for tr in trs[1:]:

            tds = tr.find_all("td")
            print(tds)
            if len(tds)==5:
                name = tds[0].text.replace(' ','').replace("\n",'')
                metric = tds[1].text.replace(' ','').replace("\n",'')
                va1 = tds[2].text.replace(' ','').replace("\n",'')
                va2 = tds[3].text.replace(' ','').replace("\n",'')
                va3 = tds[4].text.replace(' ','').replace("\n",'')
                dict_data = {"name":name,"metric":metric,"time":date1,"value":va1}
                datas.append(dict_data)
                dict_data = {"name": name, "metric": metric, "time": date2, "value": va2}
                datas.append(dict_data)
                dict_data = {"name": name, "metric": metric, "time": date3, "value": va3}
                datas.append(dict_data)
        print(datas)



def date_convert(text):
    ret = re.findall(r'(\d+)', text)

    if len(ret)>0:
        num = int(ret[0])
        time = None
        if "分钟" in text:
            time = datetime.now()-timedelta(minutes=num)
        if "小时" in text:
            time = datetime.now() - timedelta(hours=num)
        if "天" in text:
            time = datetime.now() - timedelta(days=num)
        if time is not None:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S")
            return time_str


def get_sun_data():
    url = 'https://solar.in-en.com/'
    news = get_mongo_table(database='stock', collection='news')
    respond = requests.get(url,headers={"user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36","accept-language":"an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div",'bd')
    headers = search_div[1]
    lis = headers.find_all('li')
    datas = []
    for li in lis:
        span = li.find_all("span")[0]
        a = li.find_all('a')[0]
        time_text = span.text.replace('\n',"").replace(' ','')
        header_text = a.text.replace('\n',"").replace(' ','')
        time = date_convert(time_text)
        if time is None:
            time = time_text

        dict_data = {
            "time":time,
            "header":header_text,
            "data_type":"solar_news",
            "day":time[0:10]
        }
        print(dict_data)
        datas.append(UpdateOne(
            {"header": dict_data['header'], "day": dict_data['day'], "data_type": dict_data['data_type']},
            {"$set": dict_data},
            upsert=True))
    mongo_bulk_write_data(news,datas)

def find_data():
    news = get_mongo_table(database='stock', collection='news')
    datas = []
    for ele in news.find({"data_type":"goods_news","name":"纯苯"},projection={'_id': False}).sort("time"):
        datas.append(ele)
        print(ele)



if __name__ == '__main__':
    get_sun_data()
    get_all_detail_data()
    find_data()