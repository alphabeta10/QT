import requests
from big_models.google_api import simple_big_gen_model_fn
from utils.tool import load_json_data
import google.generativeai as genai
from utils.actions import try_get_action
from utils.send_msg import MailSender
import schedule
import time
from monitor.real_common import common_filter_data
from bs4 import BeautifulSoup


def load_google_model():
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    genai.configure(api_key=api_key, transport='rest')
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:
            print(m.name)
    model = genai.GenerativeModel(version)
    return model


def handle_csrc_news():
    """
    证监会新闻监控
    :return:
    """
    url = 'http://www.csrc.gov.cn/searchList/a1a078ee0bc54721ab6b148884c784a8?_isAgg=true&_isJson=true&_pageSize=18&_template=index&_rangeTimeGte=&_channelName=&page=1'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    filter_dict_data  = {}
    filter_dict_data.setdefault("证监会",[])
    if respond.status_code == 200:
        json_data = respond.json()
        results = json_data['data']['results']
        if len(results) > 0:
            for result in results:
                content = result['content']
                title = result['title']
                publish_time = result['publishedTimeStr']
                filter_dict_data['证监会'].append({"time": publish_time, "title": title, "content": content})
            filter_dict_data = common_filter_data(filter_dict_data, 'csrc_new.txt', 'title')
            return filter_dict_data
    return {}


def handle_overseas_sanctions_news_monitor():
    """
    商务部新闻监控
    :return:
    """
    header_url = 'http://www.mofcom.gov.cn'
    url = 'http://www.mofcom.gov.cn/article/xwfb/xwfyrth/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    uls = soup.find_all("ul", 'txtList_01')
    filter_dict_data = {}
    filter_dict_data.setdefault('商务部',[])
    if uls is not None and len(uls) > 0:
        ul = uls[0]
        lis = ul.find_all("li")
        if lis is not None and len(lis) > 0:
            for li in lis:
                a_s = li.find_all("a")
                spans = li.find_all("span")
                time = None
                title = None
                href = None
                if a_s is not None and len(a_s) > 0:
                    a_ = a_s[0]
                    title = a_['title']
                    href = a_['href']
                if spans is not None and len(spans) > 0:
                    time = spans[0].text.replace("[", "").replace("]", "")
                detail_url = header_url + href
                content = get_detail_overseas_sanctions_news(detail_url)
                filter_dict_data['商务部'].append({"time": time, "title": title, "content": content})
    filter_dict_data = common_filter_data(filter_dict_data, 'mofcom_new.txt', 'title')
    return filter_dict_data



def get_detail_overseas_sanctions_news(url):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    divs = soup.find_all("div", 'art-con art-con-bottonmLine')
    if divs is not None and len(divs)>0:
        ps = divs[0].find_all("p")
        if ps is not None and len(ps)>0:
            texts = [p.text.replace("\n","").replace(" ","") for p in ps]
            content = "\n".join(texts)
            return content

def main_import_news():
    filter_dict_data = handle_csrc_news()
    model = None
    mail_msg = ""
    if len(filter_dict_data) > 0:
        model = load_google_model()
        template = "给定问答文本，总结要点以及对中国股市的影响。文本："
        for name, list_new in filter_dict_data.items():
            mail_msg += f"<p>{name}新消息如下</p>"
            mail_msg += f"<table border=\"1\">"
            mail_msg += f"<tr><th>时间</th> <th>标题</th> <th>概要以及影响</th> </tr>"
            for ele in list_new:
                time = ele['time']
                title = ele['title']
                content = ele['content']
                request_txt = template + content
                ret_data = try_get_action(simple_big_gen_model_fn, try_count=3, model=model,
                                          request_txt=request_txt, is_ret_json=False)
                if ret_data is not None:
                    mail_msg += f"<tr><td>{time}</td> <td>{title}</td> <td>{ret_data}</td></tr>"
                else:
                    print(f"模型解析数据失败:{content}")
            mail_msg += "</table>"

    filter_dict_data = handle_overseas_sanctions_news_monitor()
    if len(filter_dict_data) > 0:
        if model is None:
            model = load_google_model()
        template = "给定问答文本，总结要点以及对中国经济的影响。文本："
        for name, list_new in filter_dict_data.items():
            mail_msg += f"<p>{name}新消息如下</p>"
            mail_msg += f"<table border=\"1\">"
            mail_msg += f"<tr><th>时间</th> <th>标题</th> <th>概要以及影响</th> </tr>"
            for ele in list_new:
                time = ele['time']
                title = ele['title']
                content = ele['content']
                request_txt = template + content
                ret_data = try_get_action(simple_big_gen_model_fn, try_count=3, model=model,
                                          request_txt=request_txt, is_ret_json=False)
                if ret_data is not None:
                    mail_msg += f"<tr><td>{time}</td> <td>{title}</td> <td>{ret_data}</td></tr>"
                else:
                    print(f"模型解析数据失败:{content}")
            mail_msg += "</table>"

    if mail_msg != '':
        sender = MailSender()
        sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], "证监会新闻以及商务部数据监控", mail_msg)
        sender.close()
        print("发送数据")



if __name__ == '__main__':
    main_import_news()
    schedule.every(60).minutes.do(main_import_news)
    while True:
        schedule.run_pending()
        time.sleep(10)
