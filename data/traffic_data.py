import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt
import akshare as ak
from utils.actions import try_get_action

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')

cur_month = datetime.now().month
cur_year = datetime.now().year


def get_result_data(url, year, last_year):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    divs = soup.find_all("div", "view TRS_UEDITOR trs_paper_default trs_web")
    text_data = divs[0].text
    print(text_data)
    re_month = re.findall("(\d+)月(\d+)日\\，", text_data)
    if len(re_month) > 0:
        month, day = int(re_month[0][0]), int(re_month[0][1])
        if month < 10:
            t_month = f"0{month}"
        else:
            t_month = month
        if day < 10:
            t_day = f"0{day}"
        else:
            t_day = day

        if month > cur_month:
            if year < cur_year:
                time = f"{year}{t_month}{t_day}"
            else:
                time = f"{last_year}{t_month}{t_day}"
        else:
            time = f"{year}{t_month}{t_day}"
        split_datas = text_data.split(">>")
        tl_result = 0
        gs_result = 0
        mh_hb_result = 0
        hy_hb_result = 0
        gj_hb_result = 0
        gn_hb_result = 0
        lj_result = 0
        td_result = 0
        gk_result = 0
        gk_teu_result = 0
        for ele in split_datas[1:]:
            if "铁路货运" in ele:
                get_res = re.findall("(\d+\\.?\d+)万吨", ele)
                tl_result = get_res[0]
            if "高速公路" in ele:
                get_res = re.findall("(\d+\\.?\d+)万辆", ele)
                if len(get_res) > 0:
                    gs_result = get_res[0]
            if "港口" in ele:
                get_res = re.findall("(\d+\\.?\d+)万吨", ele)
                gk_result = get_res[0]
                get_teu_res = re.findall("(\d+\\.?\d+)万TEU", ele)
                gk_teu_result = get_teu_res[0]
            if "航班" in ele:
                get_mh_res = re.findall("民航保障航班(\d+\\.?\d+)班", ele)
                get_hy_res = re.findall("货运航班(\d+\\.?\d+)班", ele)
                get_gj_res = re.findall("国际货运航班(\d+\\.?\d+)班", ele)
                get_gn_res = re.findall("国内货运航班(\d+\\.?\d+)班", ele)
                if len(get_mh_res) > 0:
                    mh_hb_result = get_mh_res[0]
                if len(get_hy_res) > 0:
                    hy_hb_result = get_hy_res[0]
                if len(get_gj_res) > 0:
                    gj_hb_result = get_gj_res[0]
                if len(get_gn_res) > 0:
                    gn_hb_result = get_gn_res[0]
            if "邮政" in ele:
                get_lj_res = re.findall("揽收量约(\d+\\.?\d+)亿件", ele)
                get_td_res = re.findall("投递量约(\d+\\.?\d+)亿件", ele)
                if len(get_lj_res) > 0 and len(get_td_res) > 0:
                    lj_result = get_lj_res[0]
                    td_result = get_td_res[0]
                else:
                    get_res = re.findall("(\d+\\.?\d+)亿件", ele)
                    lj_result = get_res[0]
                    td_result = get_res[1]
        dict_data = {"tl_traffic": tl_result, "gs_traffic": gs_result,
                     "mh_hb_traffic": mh_hb_result, "hy_hb_traffic": hy_hb_result,
                     "gj_hb_traffic": gj_hb_result, "gn_hb_traffic": gn_hb_result,
                     "lj_traffic": lj_result, "td_traffic": td_result,
                     "gk_traffic": gk_result, "gk_teu_traffic": gk_teu_result, "time": time
                     }
        return dict_data


def show_none_data(val, text, data):
    if val is None or val == 0:
        print(text, data)
        raise Exception("解析失败")


def get_week_result_data(url, year, last_year):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    divs = soup.find_all("div", "view TRS_UEDITOR trs_paper_default trs_web")
    text_data = divs[0].text
    print(text_data)
    re_month = re.findall("(\d+)月(\d+)日-(\d+)月(\d+)日", text_data)
    print(re_month)
    if len(re_month) <= 0:
        re_month = re.findall("(\d+)月(\d+)日", text_data)
    if len(re_month) > 0:
        month, day = int(re_month[-1][-2]), int(re_month[-1][-1])
        if month < 10:
            t_month = f"0{month}"
        else:
            t_month = month
        if day < 10:
            t_day = f"0{day}"
        else:
            t_day = day

        if month > cur_month:
            if year < cur_year:
                time = f"{year}{t_month}{t_day}"
            else:
                time = f"{last_year}{t_month}{t_day}"
        else:
            time = f"{year}{t_month}{t_day}"
        split_datas = text_data.split(">>")
        tl_result = 0
        gs_result = 0
        mh_hb_result = 0
        hy_hb_result = 0
        gj_hb_result = 0
        gn_hb_result = 0
        lj_result = 0
        td_result = 0
        gk_result = 0
        gk_teu_result = 0
        for ele in split_datas[1:]:
            if "铁路货运" in ele:
                get_res = re.findall("(\d+\\.?\d+)万吨", ele)
                tl_result = get_res[0]
            if '铁路' in ele:
                get_res = re.findall("(\d+\\.?\d+)万吨", ele.replace(" ", ""))
                tl_result = get_res[0]
            if "高速公路" in ele:
                get_res = re.findall("(\d+\\.?\d+)万辆", ele.replace(" ", ""))
                if len(get_res) > 0:
                    gs_result = get_res[0]
            if "港口" in ele:
                ele = ele.replace(" ", "")
                get_res = re.findall("(\d+\\.?\d+)万吨", ele)
                gk_result = get_res[0]
                get_teu_res = re.findall("(\d+\\.?\d+)万TEU", ele)
                if len(get_teu_res) == 0:
                    get_teu_res = re.findall("(\d+\\.?\d+)万标箱", ele)
                gk_teu_result = get_teu_res[0]
            if "航班" in ele:
                get_mh_res = re.findall("保障航班(\d+\\.?\d+)班", ele)
                get_hy_res = re.findall("货运航班(\d+\\.?\d+)班", ele)
                get_gj_res = re.findall("国际货运航班(\d+\\.?\d+)班", ele)
                get_gn_res = re.findall("国内货运航班(\d+\\.?\d+)班", ele)
                if len(get_mh_res) > 0:
                    mh_hb_result = get_mh_res[0]
                else:
                    get_mh_res = re.findall("保障航班(\d+\\.?\d+)万班", ele)
                    if len(get_mh_res) > 0:
                        mh_hb_result = round(float(get_mh_res[0]) * 10000, 2)
                if len(get_hy_res) > 0:
                    hy_hb_result = get_hy_res[0]
                if len(get_gj_res) > 0:
                    gj_hb_result = get_gj_res[0]
                if len(get_gn_res) > 0:
                    gn_hb_result = get_gn_res[0]
            if "邮政" in ele:
                get_lj_res = re.findall("揽收量约(\d+\\.?\d+)亿件", ele)
                get_td_res = re.findall("投递量约(\d+\\.?\d+)亿件", ele)
                if len(get_lj_res) > 0 and len(get_td_res) > 0:
                    lj_result = get_lj_res[0]
                    td_result = get_td_res[0]
                else:
                    get_res = re.findall("(\d+\\.?\d+)亿件", ele)
                    lj_result = get_res[0]
                    td_result = get_res[1]
        show_none_data(tl_result, '公路为空', text_data)
        show_none_data(gs_result, '高速为空', text_data)
        show_none_data(mh_hb_result, '民航保障航班为空', text_data)
        show_none_data(hy_hb_result, '货运航班为空', text_data)
        show_none_data(gj_hb_result, '国际航班为空', text_data)
        show_none_data(gn_hb_result, '国内航班为空', text_data)
        show_none_data(lj_result, '邮政揽件为空', text_data)
        show_none_data(td_result, '邮政投递为空', text_data)
        show_none_data(gk_result, '港股货运吨为空', text_data)
        show_none_data(gk_teu_result, '港口集装箱为空', text_data)

        dict_data = {"tl_traffic": tl_result, "gs_traffic": gs_result,
                     "mh_hb_traffic": mh_hb_result, "hy_hb_traffic": hy_hb_result,
                     "gj_hb_traffic": gj_hb_result, "gn_hb_traffic": gn_hb_result,
                     "lj_traffic": lj_result, "td_traffic": td_result,
                     "gk_traffic": gk_result, "gk_teu_traffic": gk_teu_result, "time": time
                     }
        return dict_data
    else:
        print("解析失败")
        raise Exception("解析失败")


def get_main_traffic_data(url):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    all_ul = soup.find_all("ul")
    update_request = []
    if len(all_ul) > 0:
        ul = all_ul[0]
        news_titles = ul.find_all("li", 'news_title')
        for li in news_titles:
            a_s = li.find_all("a")
            if len(a_s) > 0:
                a = a_s[0]
                href = a['href']
                year = int(href[2:][0:4])
                last_year = year - 1
                result_url = "https://www.mot.gov.cn/zhuanti/wuliubtbc/qingkuangtongbao_wuliu/" + href[2:]
                dict_data = get_week_result_data(result_url, year, last_year)
                if dict_data is not None:
                    dict_data['data_type'] = "traffic"
                    dict_data['metric_code'] = "traffic"
                    print(dict_data)
                    update_request.append(
                        UpdateOne(
                            {"data_type": dict_data['data_type'], "time": dict_data['time'],
                             "metric_code": dict_data['metric_code']},
                            {"$set": dict_data},
                            upsert=True)
                    )
    return update_request


def traffic():
    """
    公共时序数据
    data_type，
    time，
    metric_code
    :return:
    """
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for i in range(2):
        if i >= 1:
            url = f"https://www.mot.gov.cn/zhuanti/wuliubtbc/qingkuangtongbao_wuliu/index_{i}.html"
        else:
            url = 'https://www.mot.gov.cn/zhuanti/wuliubtbc/qingkuangtongbao_wuliu/index.html'
        print(url)
        update_request = get_main_traffic_data(url)
        print(update_request)
        if update_request is not None:
            mongo_bulk_write_data(stock_common, update_request)


def wci_index_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    update_request = []
    symbols = ["composite", "shanghai-rotterdam", "rotterdam-shanghai", "shanghai-los angeles", "los angeles-shanghai",
               "shanghai-genoa", "new york-rotterdam", "rotterdam-new york"]
    for symbol in symbols:
        print(f"handle {symbol}")
        drewry_wci_index_df = try_get_action(ak.drewry_wci_index, try_count=3, symbol=symbol)
        if drewry_wci_index_df is not None and len(drewry_wci_index_df) > 0:
            for index in drewry_wci_index_df.index:
                dict_data = dict(drewry_wci_index_df.loc[index])
                date = str(dict_data['date'])
                new_dict_data = {"time": date}
                new_dict_data['data_type'] = 'wci_index'
                new_dict_data['wci'] = dict_data['wci']
                new_dict_data['metric_code'] = symbol
                update_request.append(
                    UpdateOne(
                        {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                         "metric_code": new_dict_data['metric_code']},
                        {"$set": new_dict_data},
                        upsert=True)
                )
        if len(update_request) > 0:
            mongo_bulk_write_data(stock_common, update_request)


def cn_wci_index_data(url):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')
    soup = BeautifulSoup(html_doc, 'html.parser')
    all_ue_div = soup.find_all("div", 'ue_table')
    update_request = []
    if all_ue_div is not None and len(all_ue_div) > 0:
        tables = all_ue_div[0].find_all("table")
        if tables is not None and len(tables) > 0:
            table = tables[0]
            trs = table.find_all('tr')
            tds = trs[0].find_all("td")
            header = [td.text.replace("\n", "").replace(" ", "") for td in tds]
            now_year = int(header[2][0:4])
            now_month = int(header[3].replace('月', ''))
            if int(now_month) < 10:
                now_month = f"0{now_month}"
            for tr in trs[1:]:
                tds = tr.find_all('td')
                data_list = [td.text.replace("\n", "").replace(" ", "") for td in tds]
                name = data_list[0]
                time = f"{now_year}{now_month}01"
                before_year_data = data_list[1]
                cur_year_data = data_list[2]
                cur_month_data = data_list[3]
                cycle_rate = data_list[4]

                new_dict_data = {"time": time, "data_type": "cn_wci_index", "metric_code": name,
                                 "before_year_data": before_year_data,
                                 "cur_month_data": cur_month_data, "cycle_rate": cycle_rate,
                                 "cur_year_data": cur_year_data}

                update_request.append(
                    UpdateOne(
                        {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                         "metric_code": new_dict_data['metric_code']},
                        {"$set": new_dict_data},
                        upsert=True)
                )
    return update_request


def handle_cn_wci_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    common_url = 'https://www.mot.gov.cn/yunjiazhishu/chukoujizhuangxiangyjzs/'
    for i in range(1):
        if i==0:
            url = 'https://www.mot.gov.cn/yunjiazhishu/chukoujizhuangxiangyjzs/'
        else:
            url = f'https://www.mot.gov.cn/yunjiazhishu/chukoujizhuangxiangyjzs/index_{i}.html'
        respond = requests.get(url, headers={
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
        html = respond.content
        print(f"handle url {url}")
        html_doc = str(html, 'utf-8')
        soup = BeautifulSoup(html_doc, 'html.parser')
        all_ue_div = soup.find_all("a",'list-group-item')
        for a in all_ue_div:
            if '中国出口集装箱运价指数' in a['title']:
                combine_url = common_url + a['href'][2:]
                datas = cn_wci_index_data(combine_url)
                mongo_bulk_write_data(stock_common,datas)

def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "traffic", "metric_code": "traffic", "time": {"$gt": "20230319"}},
                         projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)

    data = pd_data[["mh_hb_traffic", "hy_hb_traffic",
                    "gj_hb_traffic", "gn_hb_traffic", 'time']]

    show_data(data)
    for col in ["mh_hb_traffic", "hy_hb_traffic",
                "gj_hb_traffic", "gn_hb_traffic"]:
        data[[col]] = data[[col]].astype(float)
    data.set_index(keys=['time'], inplace=True)
    # data['gk_teu_traffic_ptc'] = data['gk_teu_traffic'].pct_change(1)
    data.plot(kind='line', title='航班', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


if __name__ == '__main__':
    handle_cn_wci_data()
    traffic()
    find_data()
