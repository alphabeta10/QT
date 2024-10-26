import time
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
from selenium import webdriver
import urllib3
import matplotlib.pyplot as plt
from utils.actions import show_data

plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

urllib3.disable_warnings()
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
import warnings

warnings.filterwarnings('ignore')

get_comm_url = "https://data.stats.gov.cn/easyquery.htm"
get_yd_meta_params = {"id": "A01", "dbcode": "hgyd", "wdcode": "zb", "m": "getTree"}
get_yd_data_params = {"m": "QueryData", "dbcode": "hgyd", "rowcode": "zb", "wds": [],
                      "dfwds": [{"wdcode": "zb", "valuecode": "A0203"}], "colcode": "sj", "h": 1, "k1": 1664971768918}

get_jd_meta_params = {"id": "A01", "dbcode": "hgjd", "wdcode": "zb", "m": "getTree"}
get_jd_data_params = {"m": "QueryData", "dbcode": "hgjd", "rowcode": "zb", "wds": [],
                      "dfwds": [{"wdcode": "zb", "valuecode": "A0203"}], "colcode": "sj", "h": 1, "k1": 1664971768918}

get_nd_meta_params = {"id": "A01", "dbcode": "hgnd", "wdcode": "zb", "m": "getTree"}
get_nd_data_params = {"m": "QueryData", "dbcode": "hgnd", "rowcode": "zb", "wds": [],
                      "dfwds": [{"wdcode": "zb", "valuecode": "A0203"}], "colcode": "sj", "h": 1, "k1": 1664971768918}


def is_json(str_: str):
    try:
        json.loads(str_)
    except Exception as e:
        return False
    return True


def post_or_get_data(url, params=None, method="post"):
    if method == "post":
        result = requests.post(url, params=params, verify=False)
        text = result.text
        if is_json(text):
            json_data = json.loads(text)
            return json_data

        else:
            print(text)
    elif method == "get":
        headers = {"Cookie": "u=5; JSESSIONID=WoeodpZtJvJsNQEVHd3hiYOwplTyVAic2On59X93uxTnmzN6quMw!1294272777"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Cookie": "_trs_uv=ld8nyuch_6_10t8; wzws_sessionid=gmZjNWVlMYAxMTMuOTAuODEuMTI3oGaoONiBMTBmOTI4; u=2; JSESSIONID=RHoCr9Fu0lSZS08fdhw8L-OKs3KJ-0ITzKGEy8BG_IwYcpZ-_RaF!1681534999"}
        result = requests.get(url, params=params, headers=headers, verify=False)
        text = result.text
        if is_json(text):
            json_data = json.loads(text)
            return json_data
        else:
            print(text)
    else:
        return None


def concat_param(dict_pramas: dict):
    lt = []
    for k, v in dict_pramas.items():
        if isinstance(v, list):
            v = json.dumps(v)
        lt.append(f"{k}={v}")
    query_str = "&".join(lt)
    return query_str


def to_meta_data(dict_data, data_type: str, before_class_list: list = None):
    request_update = []
    class_dict = {}
    for i, ele in enumerate(before_class_list):
        class_dict[f'class_level_{i + 1}'] = ele
    print(class_dict)
    metric_data = dict_data['returndata']['wdnodes'][0]
    if metric_data['wdname'] == '指标':
        for node in metric_data['nodes']:
            node['code'] = str(node['code']) + "_" + data_type
            for k, v in class_dict.items():
                node[k] = v
            request_update.append(
                UpdateOne(
                    {"code": node['code']},
                    {"$set": node},
                    upsert=True)
            )
    else:
        metric_data = dict_data['returndata']['wdnodes'][1]
        if metric_data['wdname'] == '指标':
            for node in metric_data['nodes']:
                node['code'] = str(node['code']) + "_" + data_type
                for k, v in class_dict.items():
                    node[k] = v
                request_update.append(
                    UpdateOne(
                        {"code": node['code']},
                        {"$set": node},
                        upsert=True)
                )
    return request_update


def to_data(dict_data, data_type: str):
    request_update = []
    datanodes = dict_data['returndata']['datanodes']

    for node in datanodes:
        data = node['data']
        wds = node['wds']
        time = None
        code = None
        for wd in wds:
            if wd['wdcode'] == 'zb':
                code = wd['valuecode']
            if wd['wdcode'] == 'sj':
                time = wd['valuecode']
        if time is None or code is None:
            print('no code or time')
            return
        data['code'] = code + "_" + data_type
        data['time'] = time
        request_update.append(
            UpdateOne(
                {"code": data['code'], "time": data['time']},
                {"$set": data},
                upsert=True)
        )
    return request_update


def rec_get_data(mete_data: dict, meta_params: dict, data_params: dict, data_type: str, meta_info, data_info,
                 before_class_list: list = None):
    is_parent = mete_data['isParent']
    id = mete_data['id']
    wdcode = mete_data['wdcode']
    if is_parent is False:
        parent_class_list = []
        if before_class_list is not None:
            parent_class_list.extend(before_class_list)
        parent_class_list.append(mete_data['name'])
        data_params["dfwds"][0]["valuecode"] = id
        data_params['dfwds'][0]["wdcode"] = wdcode
        url_str = get_comm_url + "?" + concat_param(data_params)
        print(url_str)
        data = try_get_action(post_or_get_data, try_count=3, url=url_str, method="get")
        if data is not None:
            upsert_datas = to_data(data, data_type)
            if len(upsert_datas) > 0:
                update_result = data_info.bulk_write(upsert_datas, ordered=False)
                print('数据录入插入：%4d条, 更新：%4d条' % (update_result.upserted_count, update_result.modified_count),
                      flush=True)
            # upsert_datas = to_meta_data(data, data_type, parent_class_list)
            # if len(upsert_datas) > 0:
            #     update_result = meta_info.bulk_write(upsert_datas, ordered=False)
            #     print('元数据录入插入：%4d条, 更新：%4d条' % (update_result.upserted_count, update_result.modified_count),
            #           flush=True)
        time.sleep(10)


    else:
        meta_params['id'] = id
        if id not in ['A0203', 'A010C', 'A0204', 'A0206', 'A0207', 'A020M', 'A020L', 'A020N', 'A010A', 'A0109']:
            if before_class_list is not None:
                before_class_list.append(mete_data['name'])
            meta_data_rs = try_get_action(post_or_get_data, try_count=3, url=get_comm_url, params=meta_params)
            for ele_meta in meta_data_rs:
                rec_get_data(ele_meta, meta_params, data_params, data_type, meta_info, data_info,before_class_list)


def handle_gov_yd_data():
    """
    ids = ['A0209']
    ids = ['A0301']
    ids = ['A0108']
    ids = ['A0101','A0102','A0103']
    ids = ['A01']
    'A01': '价格指数',
    """
    ids = ['A01', 'A02', 'A03', 'A04', 'A05', 'A0E', 'A06', 'A07', 'A08', 'A09', 'A0A', 'A0B', 'A0C', 'A0D']
    ids = {'A01': '价格指数','A02': '工业', 'A03': '能源', 'A04': '固定资产投资(不含农户)', 'A05': '服务业生产指数',
           'A0E': '城镇调查失业率', 'A06': '房地产', 'A07': '国内贸易', 'A08': '对外经济', 'A09': '交通运输',
           'A0A': '邮电通信',
           'A0B': '采购经理指数', 'A0C': '财政', 'A0D': '金融'}
    ids = {'A02': '工业', 'A03': '能源', 'A04': '固定资产投资(不含农户)', 'A05': '服务业生产指数',
           'A0E': '城镇调查失业率', 'A06': '房地产', 'A07': '国内贸易', 'A08': '对外经济', 'A09': '交通运输',
           'A0A': '邮电通信',
           'A0B': '采购经理指数', 'A0C': '财政', 'A0D': '金融'}
    # ids = ['A03', 'A04', 'A05', 'A0E', 'A06', 'A07', 'A08', 'A09', 'A0A', 'A0B', 'A0C', 'A0D']
    # ids = ['A01', 'A02']
    # ids = ['A0B',"A01"]
    # ids = ['A01']
    data_info = get_mongo_table(database='govstats', collection='data_info')
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    data_type = "yd"
    for id, name in ids.items():
        get_yd_meta_params["id"] = id
        meta_data_list = try_get_action(post_or_get_data, try_count=10, url=get_comm_url, params=get_yd_meta_params)

        for meta_data in meta_data_list:
            print(meta_data)
            id = meta_data['id']
            if id not in ['A0203', 'A010C', 'A0204', 'A0206', 'A0207', 'A020M', 'A020L', 'A020N', 'A010A', 'A0109']:
                before_class_list = [name]
                rec_get_data(meta_data, get_yd_meta_params, get_yd_data_params, data_type, meta_info, data_info,
                             before_class_list)
            else:
                print(f"filter {id}")


def handle_gov_jd_data():
    """
    ids = ['A0209']
    ids = ['A0301']
    ids = ['A0108']
    ids = ['A0101','A0102','A0103']
    ids = ['A01']
    """
    ids = ['A01', 'A02', 'A0302', 'A04', 'A05', 'A06', 'A07', 'A08']
    ids = {'A01': '国民经济核算', 'A02': '农业', 'A03': '工业', 'A04': '建筑业', 'A05': '人民生活', 'A06': '价格指数',
           'A07': '国内贸易', 'A08': '文化'}
    #ids = {'A03': '工业', 'A04': '建筑业', 'A05': '人民生活', 'A06': '价格指数', 'A07': '国内贸易', 'A08': '文化'}
    data_info = get_mongo_table(database='govstats', collection='data_info')
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    data_type = "jd"
    for id, name in ids.items():
        get_jd_meta_params["id"] = id
        meta_data_list = try_get_action(post_or_get_data, 4, url=get_comm_url, params=get_jd_meta_params)
        for meta_data in meta_data_list:
            print(meta_data)
            jd_parent_filter = {"jd": ['A0301']}
            if data_type == 'jd':
                id = meta_data['id']
                if id in jd_parent_filter.get('jd'):
                    print(f"filter jb {id}")
                    continue
            before_class_list = [name]
            rec_get_data(meta_data, get_jd_meta_params, get_jd_data_params, data_type, meta_info, data_info,
                         before_class_list)

def handle_gov_nd_data():
    """
    ids = ['A0209']
    ids = ['A0301']
    ids = ['A0108']
    ids = ['A0101','A0102','A0103']
    ids = ['A01']
    """
    ids = ['A01', 'A02', 'A0302', 'A04', 'A05', 'A06', 'A07', 'A08']
    ids = {'A01': '综合', 'A02': '国民经济核算', 'A03': '人口', 'A04': '就业人员和工资',
           'A05': '固定资产投资和房地产', 'A06': '对外经济贸易',
           'A07': '能源', 'A08': '财政',
           'A09': '价格指数', 'A0A': '人民生活',
           'A0B': '城市概况', 'A0C': '资源和环境',
           'A0D': '农业', 'A0E': '工业',
           'A0F': '建筑业', 'A0G': '运输和邮电',
           'A0H': '社会消费品零售总额', 'A0I': '批发和零售业',
           'A0J': '住宿和餐饮', 'A0K': '旅游业',
           'A0L': '金融业', 'A0M': '教育',
           'A0N': '科技', 'A0O': '卫生',
           'A0P': '社会服务', 'A0Q': '文化',
           'A0R': '体育', 'A0S': '公共管理，社会保障及其他',
           }
    #ids = {'A03': '工业', 'A04': '建筑业', 'A05': '人民生活', 'A06': '价格指数', 'A07': '国内贸易', 'A08': '文化'}
    data_info = get_mongo_table(database='govstats', collection='data_info')
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    data_type = "nd"
    for id, name in ids.items():
        get_nd_meta_params["id"] = id
        meta_data_list = try_get_action(post_or_get_data, 4, url=get_comm_url, params=get_nd_meta_params)
        for meta_data in meta_data_list:
            print(meta_data)
            jd_parent_filter = {"jd": ['A0301']}
            if data_type == 'jd':
                id = meta_data['id']
                if id in jd_parent_filter.get('jd'):
                    print(f"filter jb {id}")
                    continue
            before_class_list = [name]
            rec_get_data(meta_data, get_nd_meta_params, get_nd_data_params, data_type, meta_info, data_info,
                         before_class_list)

def handle_gov_tmp_data():
    """
    ids = ['A0209']
    ids = ['A0301']
    ids = ['A0108']
    ids = ['A0101','A0102','A0103']
    ids = ['A01']
    'A01': '价格指数',
    """
    ids = ['A01', 'A02', 'A03', 'A04', 'A05', 'A0E', 'A06', 'A07', 'A08', 'A09', 'A0A', 'A0B', 'A0C', 'A0D']
    ids = {'A01': '价格指数','A02': '工业', 'A03': '能源', 'A04': '固定资产投资(不含农户)', 'A05': '服务业生产指数',
           'A0E': '城镇调查失业率', 'A06': '房地产', 'A07': '国内贸易', 'A08': '对外经济', 'A09': '交通运输',
           'A0A': '邮电通信',
           'A0B': '采购经理指数', 'A0C': '财政', 'A0D': '金融'}
    ids = {'A0208': '工业'}
    ids = {'A0401': '工业'}
    # ids = ['A03', 'A04', 'A05', 'A0E', 'A06', 'A07', 'A08', 'A09', 'A0A', 'A0B', 'A0C', 'A0D']
    # ids = ['A01', 'A02']
    # ids = ['A0B',"A01"]
    # ids = ['A01']
    data_info = get_mongo_table(database='govstats', collection='data_info')
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    data_type = "yd"
    is_get_meta = False
    if not is_get_meta:
        for id in ids.keys():
            meta_data = {"id":id,"wdcode":"zb","isParent":False,"name":"default"}
            name = "default"
            print(meta_data)
            id = meta_data['id']
            if id not in ['A0203', 'A010C', 'A0204', 'A0206', 'A0207', 'A020M', 'A020L', 'A020N', 'A010A', 'A0109']:
                before_class_list = [name]
                rec_get_data(meta_data, get_yd_meta_params, get_yd_data_params, data_type, meta_info, data_info,
                             before_class_list)
            else:
                print(f"filter {id}")
    else:
        for id, name in ids.items():
            get_yd_meta_params["id"] = id
            meta_data_list = try_get_action(post_or_get_data, try_count=10, url=get_comm_url, params=get_yd_meta_params)

            for meta_data in meta_data_list:
                print(meta_data)
                id = meta_data['id']
                if id not in ['A0203', 'A010C', 'A0204', 'A0206', 'A0207', 'A020M', 'A020L', 'A020N', 'A010A', 'A0109']:
                    before_class_list = [name]
                    rec_get_data(meta_data, get_yd_meta_params, get_yd_data_params, data_type, meta_info, data_info,
                                 before_class_list)
                else:
                    print(f"filter {id}")



def find_meta_by_code_regex(code_regex=None):
    if code_regex is None:
        code_regex = {"$regex": "A020O09"}
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    dict_data = {}
    for ele in meta_info.find({"code": code_regex}, projection={'_id': False}).sort("code").limit(10000):
        code = ele['code']
        cname = ele['cname']
        dict_data[code] = cname
    return dict_data



def energy_cov_data():
    code_dict = find_meta_by_code_regex({"$regex": "A02092E"})
    code_dict = {code: cname for code, cname in code_dict.items() if "yd" in code and '累计增长' in cname}

    pro_code_dict = find_meta_by_code_regex()
    pro_code_dict = {code: cname for code, cname in pro_code_dict.items() if "yd" in code and '累计增长' in cname}

    for k, v in pro_code_dict.items():
        code_dict[k] = v

    data_info = get_mongo_table(database='govstats', collection='data_info')
    datas = []
    code_list = {"$in": list(code_dict.keys())}
    for ele in data_info.find({"code": code_list}, projection={'_id': False}):
        ele['data'] = float(ele['data'])
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    pd_data = pd.pivot_table(pd_data, values='data', index=['time'], columns=['code'])
    # pd_data.dropna(axis=0,inplace=True)
    pd_data.rename(columns=code_dict, inplace=True)
    pd_data.sort_index(inplace=True)
    corr = pd_data.corr()

def create_db_index():
    data_info = get_mongo_table(database='govstats', collection='data_info')
    data_info.create_index([("code", 1), ("time", 1)], unique=True, background=True)


if __name__ == '__main__':
    # find_mata_data()
    # find_all_data()
    #handle_gov_jd_data()
    # find_mata_data()
    handle_gov_yd_data()
    #handle_gov_tmp_data()
    # energy_cov_data()
    # find_data()
