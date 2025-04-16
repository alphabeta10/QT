"""
实时获取网络新闻数据
"""
import requests
from bs4 import BeautifulSoup
import akshare as ak
from utils.actions import try_get_action


HEADERS={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"}
def get_li_price_data(name,url='https://www.100ppi.com/vane/detail-733.html'):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div",'list-event')

    divs = soup.find_all("div","colunm pr-bor")
    sell_url = None
    if divs and len(divs)>0:
        pr_div = divs[2]
        spans = pr_div.find_all("span")
        if spans and len(spans)>0:
            a_s = spans[0].find_all('a')
            if a_s and len(a_s)>0:
                sell_url = a_s[0]['href']

    headers = search_div[0]
    lis = headers.find_all('li')
    comm_head = 'http://www.100ppi.com'
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
        new_url = f"{comm_head}{a.attrs['href']}"
        dict_data = {"time":time_text,"header":header_text,"data_type":data_type,"name":name,"url":new_url}
        datas.append(dict_data)
    return datas,sell_url


def get_100ppi_detail_new_data(url):
    """
    获取生意社新闻详细数据
    :param url:
    :return:
    """

    respond = requests.get(url,headers=HEADERS)
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div", 'nd-c width588')
    if search_div and len(search_div)>0:
        detail_div = search_div[0]
        return detail_div.text.replace(' ', '').replace("\n", '')
    return None


def get_all_detail_data(names=None):
    url = 'https://www.100ppi.com/monitor2/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div", 'right fl')

    headers = search_div[0]
    header_url = "http://www.100ppi.com"
    tables = headers.find_all('table')
    ret_news = {}
    sell_urls = {}
    for table in tables:
        trs = table.find_all('tr')
        for tr in trs[1:]:
            urls = tr.find_all("a")
            href = urls[0].get("href")

            if ".html" in href:
                url = f"{header_url}{href}"
                name = urls[0].text.replace(' ', '').replace("\n", '')
                if names is not None and name in names:
                    try:
                        x,sell_url = try_get_action(get_li_price_data,try_count=3,name=name,url=url)
                        ret_news[name] = x
                        sell_urls[name] = sell_url
                    except Exception as e:
                        print(e)
                        print(url)
    return ret_news,sell_urls
def stock_telegraph_cls_news():
    stock_telegraph_cls_df = try_get_action(ak.stock_info_global_cls,try_count=3)
    datas = []
    filter_dup = set()
    for index in stock_telegraph_cls_df.index:
        data = dict(stock_telegraph_cls_df.loc[index])
        pub_time = str(data['发布时间'])
        pub_day = str(data['发布日期'])
        title = data['标题']
        content = data['内容']
        key = f"{title}_{content}"
        dict_data = {"time":f"{pub_day} {pub_time}","day":pub_day,"title":title,"content":content,"data_type":"cls_telegraph"}
        if key not in filter_dup:
            datas.append(dict_data)
        filter_dup.add(key)
    return datas


if __name__ == '__main__':
    news,url = get_all_detail_data(names=['碳酸锂','轻质纯碱','玻璃'])
    print(news,url)