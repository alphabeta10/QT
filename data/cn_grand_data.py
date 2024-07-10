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

"""
m: QueryData
dbcode: hgyd
rowcode: zb
colcode: sj
wds: []
dfwds: [{"wdcode":"zb","valuecode":"A0203"}]
k1: 1650213417995
h: 1
"""

"https://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{%22wdcode%22:%22zb%22,%22valuecode%22:%22A010101%22}]"


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
            "Cookie": "_trs_uv=ld8nyuch_6_10t8; wzws_sessionid=gTEwZjkyOIAxNC4xNTMuNC44MIJmYzVlZTGgZoy0Pg==; JSESSIONID=lKSVn1_eaeQ_Ite_BMn0n75otCWxnJefgJj_y5Dl5HAssUYEl5pX!-340470345; u=5"}
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
    ids = {'A0B': '采购经理指数'}
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


def find_mata_data():
    meta_info = get_mongo_table(database='govstats', collection='meta_info')
    dict_data = {}
    # A020O0501 存货
    # A020O053R
    for ele in meta_info.find({"code": {"$regex": "A020O05"}}, projection={'_id': False}).sort("code").limit(10000):
        code = ele['code']
        cname = ele['cname']
        if "yd" in code and '存货_增减' in cname:
            dict_data[code] = cname
            print(ele)
    print(dict_data)


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


def find_data():
    code = 'A03010L01'
    code = 'A03010K01'
    code = 'A03010J02'
    code = 'A03010G01'
    code = 'A03010J01'
    code = 'A02092E01'
    code = 'A02092D01'
    code = 'A010501_jd'  # 最终消费支出对国内生产总值增长贡献率_当季值

    code_dict = {
        'A010101_jd': '国内生产总值_当季值',
        'A010103_jd': '第一产业增加值_当季值',
        'A010105_jd': '第二产业增加值_当季值',
        'A010107_jd': '第三产业增加值_当季值',
        'A010109_jd': '农林牧渔业增加值_当季值',
    }
    # 总体分析code 和名称
    code_dict = {
        "A010501_jd": "最终消费支出对国内生产总值增长贡献率_当季值",
        "A010503_jd": "资本形成总额对国内生产总值增长贡献率_当季值",
        "A010505_jd": "货物和服务净出口对国内生产总值增长贡献率_累计值",
    }
    # 消费数据月分析'A07040101_yd': '粮油、食品、饮料、烟酒类商品零售类值_当期值',
    # 粮油、食品类商品 >烟酒>饮料大
    code_dict = {'A07040105_yd': '粮油、食品类商品零售类值_当期值',
                 'A0704010H_yd': '饮料类商品零售类值_当期值', 'A0704010L_yd': '烟酒类商品零售类值_当期值'}
    # 同比增长  粮油、食品类商品零售类没有出现负增长，但饮料和烟酒有负增长，疫情原因，少消费，关注基本吃
    code_dict = {'A07040103_yd': '粮油、食品、饮料、烟酒类商品零售类值_同比增长',
                 'A07040107_yd': '粮油、食品类商品零售类值_同比增长',
                 'A0704010J_yd': '饮料类商品零售类值_同比增长', 'A0704010N_yd': '烟酒类商品零售类值_同比增长'}
    # 穿的同步数据
    code_dict = {'A07040203_yd': '服装鞋帽、针、纺织品类商品零售类值_同比增长',
                 'A07040207_yd': '服装类商品零售类值_同比增长'}
    code_dict = {'A02092E03_yd': '太阳能电池（光伏电池）产量_同比增长',
                 'A02092E04_yd': '太阳能电池（光伏电池）产量_累计增长'}
    code_dict = {'A03010L01_yd': "太阳能发电量_当期值", 'A03010L02_yd': '太阳能发电量_累计值'}
    code_dict = {'A02090804_yd': "乳制品产量_累计增长"}
    code_dict = {'A02090A04_yd': "啤酒产量_累计增长"}
    # code = 'A010502_jd' #最终消费支出对国内生产总值增长贡献率_累计值
    data_info = get_mongo_table(database='govstats', collection='data_info')
    datas = []
    for code, name in code_dict.items():
        for ele in data_info.find({"code": code}, projection={'_id': False}).sort("time"):
            ele['data'] = float(ele['data'])
            print(ele)
            datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    pd_data = pd.pivot_table(pd_data, values='data', index=['time'], columns=['code'])
    pd_data = pd_data.rename(columns=code_dict)
    data = pd_data
    data.plot(kind='bar', title='数据展示', rot=45, width=0.5)
    # data.plot(kind='line', title='gdp相关的数据')
    plt.show()


def find_all_data():
    code_dict = {'A02090104_yd': '铁矿石原矿产量_累计增长', 'A02090204_yd': '磷矿石（折含五氧化二磷30％）产量_累计增长',
                 'A02090304_yd': '原盐产量_累计增长',
                 'A02090404_yd': '饲料产量_累计增长', 'A02090504_yd': '精制食用植物油产量_累计增长',
                 'A02090604_yd': '成品糖产量_累计增长',
                 'A02090704_yd': '鲜、冷藏肉产量_累计增长', 'A02090804_yd': '乳制品产量_累计增长',
                 'A02090904_yd': '白酒（折65度，商品量）产量_累计增长',
                 'A02090A04_yd': '啤酒产量_累计增长', 'A02090B04_yd': '葡萄酒产量_累计增长',
                 'A02090C04_yd': '饮料产量_累计增长',
                 'A02090D04_yd': '卷烟产量_累计增长', 'A02090E04_yd': '纱产量_累计增长',
                 'A02090F04_yd': '布产量_累计增长',
                 'A02090G04_yd': '蚕丝及交织机织物（含蚕丝≥30％）产量_累计增长',
                 'A02090H04_yd': '机制纸及纸板（外购原纸加工除外）产量_累计增长',
                 'A02090I04_yd': '新闻纸产量_累计增长', 'A02090J04_yd': '硫酸（折100％）产量_累计增长',
                 'A02090K04_yd': '烧碱（折100％）产量_累计增长',
                 'A02090L04_yd': '纯碱（碳酸钠）产量_累计增长', 'A02090M04_yd': '乙烯产量_累计增长',
                 'A02090N04_yd': '农用氮、磷、钾化学肥料（折纯）产量_累计增长',
                 'A02090O04_yd': '化学农药原药（折有效成分100％）产量_累计增长',
                 'A02090P04_yd': '初级形态塑料产量_累计增长', 'A02090Q04_yd': '合成橡胶产量_累计增长',
                 'A02090R04_yd': '合成洗涤剂产量_累计增长',
                 'A02090S04_yd': '化学药品原药产量_累计增长', 'A02090T04_yd': '中成药产量_累计增长',
                 'A02090U04_yd': '化学纤维产量_累计增长',
                 'A02090V04_yd': '合成纤维产量_累计增长', 'A02090W04_yd': '橡胶轮胎外胎产量_累计增长',
                 'A02090X04_yd': '塑料制品产量_累计增长',
                 'A02090Y04_yd': '水泥产量_累计增长', 'A02090Z04_yd': '平板玻璃产量_累计增长',
                 'A02091004_yd': '钢化玻璃产量_累计增长',
                 'A02091104_yd': '夹层玻璃产量_累计增长', 'A02091204_yd': '中空玻璃产量_累计增长',
                 'A02091304_yd': '生铁产量_累计增长',
                 'A02091404_yd': '粗钢产量_累计增长', 'A02091504_yd': '钢材产量_累计增长',
                 'A02091604_yd': '钢筋产量_累计增长',
                 'A02091704_yd': '线材（盘条）产量_累计增长', 'A02091804_yd': '冷轧薄板产量_累计增长',
                 'A02091904_yd': '中厚宽钢带产量_累计增长',
                 'A02091A04_yd': '焊接钢管产量_累计增长', 'A02091B04_yd': '铁合金产量_累计增长',
                 'A02091C04_yd': '氧化铝产量_累计增长',
                 'A02091D04_yd': '十种有色金属产量_累计增长', 'A02091E04_yd': '精炼铜（电解铜）产量_累计增长',
                 'A02091F04_yd': '铅产量_累计增长',
                 'A02091G04_yd': '锌产量_累计增长', 'A02091H04_yd': '原铝（电解铝）产量_累计增长',
                 'A02091I04_yd': '铝合金产量_累计增长',
                 'A02091J04_yd': '铜材产量_累计增长', 'A02091K04_yd': '铝材产量_累计增长',
                 'A02091L04_yd': '金属集装箱产量_累计增长',
                 'A02091M04_yd': '工业锅炉产量_累计增长', 'A02091N04_yd': '发动机产量_累计增长',
                 'A02091O04_yd': '金属切削机床产量_累计增长',
                 'A02091P04_yd': '金属成形机床产量_累计增长', 'A02091Q04_yd': '电梯、自动扶梯及升降机产量_累计增长',
                 'A02091R04_yd': '电动手提式工具产量_累计增长', 'A02091S04_yd': '包装专用设备产量_累计增长',
                 'A02091T04_yd': '复印和胶版印制设备产量_累计增长',
                 'A02091U04_yd': '挖掘机产量_累计增长', 'A02091V04_yd': '水泥专用设备产量_累计增长',
                 'A02091W04_yd': '金属冶炼设备产量_累计增长',
                 'A02091X04_yd': '饲料生产专用设备产量_累计增长', 'A02091Y04_yd': '大型拖拉机产量_累计增长',
                 'A02091Z04_yd': '中型拖拉机产量_累计增长',
                 'A02092004_yd': '小型拖拉机产量_累计增长', 'A02092104_yd': '大气污染防治设备产量_累计增长',
                 'A02092204_yd': '工业机器人产量_累计增长',
                 'A02092304_yd': '汽车产量_累计增长', 'A02092404_yd': '基本型乘用车（轿车）产量_累计增长',
                 'A02092504_yd': '运动型多用途乘用车（SUV）产量_累计增长', 'A02092604_yd': '载货汽车产量_累计增长',
                 'A02092704_yd': '铁路机车产量_累计增长',
                 'A02092804_yd': '动车组产量_累计增长', 'A02092904_yd': '民用钢质船舶产量_累计增长',
                 'A02092A04_yd': '发电机组（发电设备）产量_累计增长',
                 'A02092B04_yd': '交流电动机产量_累计增长', 'A02092C04_yd': '光缆产量_累计增长',
                 'A02092D04_yd': '锂离子电池产量_累计增长',
                 'A02092E04_yd': '太阳能电池（光伏电池）产量_累计增长',
                 'A02092F04_yd': '家用电冰箱（家用冷冻冷藏箱）产量_累计增长',
                 'A02092G04_yd': '家用冷柜（家用冷冻箱）产量_累计增长', 'A02092H04_yd': '房间空气调节器产量_累计增长',
                 'A02092I04_yd': '家用洗衣机产量_累计增长',
                 'A02092J04_yd': '电子计算机整机产量_累计增长', 'A02092K04_yd': '微型计算机设备产量_累计增长',
                 'A02092L04_yd': '程控交换机产量_累计增长',
                 'A02092M04_yd': '移动通信基站设备产量_累计增长', 'A02092N04_yd': '传真机产量_累计增长',
                 'A02092O04_yd': '移动通信手持机（手机）产量_累计增长',
                 'A02092P04_yd': '彩色电视机产量_累计增长', 'A02092Q04_yd': '集成电路产量_累计增长',
                 'A02092R04_yd': '光电子器件产量_累计增长',
                 'A02092S04_yd': '电工仪器仪表产量_累计增长', 'A02092T04_yd': '挖掘铲土运输机械产量_累计增长',
                 'A02092U04_yd': '服务机器人产量_累计增长',
                 'A02092V04_yd': '智能手表产量_累计增长', 'A02092W04_yd': '新能源汽车产量_累计增长',
                 'A02092X04_yd': '智能手机产量_累计增长'}

    """
    AI 电子计算机相关东西
    """
    code_dict = {"A02080Z01_yd": "计算机、通信和其他电子设备制造业出口交货值_当期值",
                 "A02080Z02_yd": "计算机、通信和其他电子设备制造业出口交货值_累计值",
                 "A02080Z03_yd": "计算机、通信和其他电子设备制造业出口交货值_同比增长",
                 "A02080Z04_yd": "计算机、通信和其他电子设备制造业出口交货值_累计增长"}

    code_dict = {"A02092J01_yd": "电子计算机整机产量_当期值",
                 "A02092J02_yd": "电子计算机整机产量_累计值",
                 "A02092J03_yd": "电子计算机整机产量_同比增长",
                 "A02092J04_yd": "电子计算机整机产量_累计增长"}

    code_dict = {'A020O0903_yd': '工业企业营业收入_累计增长', 'A020O0906_yd': '采矿业营业收入_累计增长',
                 'A020O0909_yd': '煤炭开采和洗选业营业收入_累计增长',
                 'A020O090C_yd': '石油和天然气开采业营业收入_累计增长',
                 'A020O090F_yd': '黑色金属矿采选业营业收入_累计增长',
                 'A020O090I_yd': '有色金属矿采选业营业收入_累计增长',
                 'A020O090L_yd': '非金属矿采选业营业收入_累计增长',
                 'A020O090O_yd': '开采专业及辅助性活动营业收入_累计增长',
                 'A020O090R_yd': '其他采矿业营业收入_累计增长', 'A020O090U_yd': '制造业营业收入_累计增长',
                 'A020O090X_yd': '农副食品加工业营业收入_累计增长', 'A020O0910_yd': '食品制造业营业收入_累计增长',
                 'A020O0913_yd': '酒、饮料和精制茶制造业营业收入_累计增长',
                 'A020O0916_yd': '烟草制品业营业收入_累计增长',
                 'A020O0919_yd': '纺织业营业收入_累计增长', 'A020O091C_yd': '纺织服装、服饰业营业收入_累计增长',
                 'A020O091F_yd': '皮革、毛皮、羽毛及其制品和制鞋业营业收入_累计增长',
                 'A020O091I_yd': '木材加工和木、竹、藤、棕、草制品业营业收入_累计增长',
                 'A020O091L_yd': '家具制造业营业收入_累计增长',
                 'A020O091O_yd': '造纸和纸制品业营业收入_累计增长',
                 'A020O091R_yd': '印刷和记录媒介复制业营业收入_累计增长',
                 'A020O091U_yd': '文教、工美、体育和娱乐用品制造业营业收入_累计增长',
                 'A020O091X_yd': '石油、煤炭及其他燃料加工业营业收入_累计增长',
                 'A020O0920_yd': '化学原料和化学制品制造业营业收入_累计增长',
                 'A020O0923_yd': '医药制造业营业收入_累计增长',
                 'A020O0926_yd': '化学纤维制造业营业收入_累计增长', 'A020O0929_yd': '橡胶和塑料制品业营业收入_累计增长',
                 'A020O092C_yd': '非金属矿物制品业营业收入_累计增长',
                 'A020O092F_yd': '黑色金属冶炼和压延加工业营业收入_累计增长',
                 'A020O092I_yd': '有色金属冶炼和压延加工业营业收入_累计增长',
                 'A020O092L_yd': '金属制品业营业收入_累计增长',
                 'A020O092O_yd': '通用设备制造业营业收入_累计增长', 'A020O092R_yd': '专用设备制造业营业收入_累计增长',
                 'A020O092U_yd': '汽车制造业营业收入_累计增长',
                 'A020O092X_yd': '铁路、船舶、航空航天和其他运输设备制造业营业收入_累计增长',
                 'A020O0930_yd': '电气机械和器材制造业营业收入_累计增长',
                 'A020O0933_yd': '计算机、通信和其他电子设备制造业营业收入_累计增长',
                 'A020O0936_yd': '仪器仪表制造业营业收入_累计增长', 'A020O0939_yd': '其他制造业营业收入_累计增长',
                 'A020O093C_yd': '废弃资源综合利用业营业收入_累计增长',
                 'A020O093F_yd': '金属制品、机械和设备修理业营业收入_累计增长',
                 'A020O093I_yd': '电力、热力、燃气及水生产和供应业营业收入_累计增长',
                 'A020O093L_yd': '电力、热力生产和供应业营业收入_累计增长',
                 'A020O093O_yd': '燃气生产和供应业营业收入_累计增长',
                 'A020O093R_yd': '水的生产和供应业营业收入_累计增长'}

    data_info = get_mongo_table(database='govstats', collection='data_info')
    datas = []
    st_gt = {"gt": 0, "lt": 0}
    code_list = {"$in": list(code_dict.keys())}
    for ele in data_info.find({"code": code_list, "time": "202307"}, projection={'_id': False}).sort("time"):
        ele['data'] = float(ele['data'])
        ele['cn_name'] = code_dict.get(ele['code'])
        datas.append(ele)
        if ele['data'] > 0:
            st_gt["gt"] += 1
        else:
            st_gt['lt'] += 1
    print(st_gt)
    pd_data = pd.DataFrame(data=datas)
    pd_data.sort_values(by='data', inplace=True)
    pd_data = pd_data[['cn_name', 'time', 'data']]
    pd_data.to_csv("industry_revenue.csv", index=False)


def sort_dict_data_by(dict_data, by='key'):
    import operator
    if by == 'key':
        return dict(sorted(dict_data.items(), key=operator.itemgetter(0)))  # 按照key值升序
    else:
        return dict(sorted(dict_data.items(), key=operator.itemgetter(1)))  # 按照value值升序


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
    for index in corr.index:
        print(sort_dict_data_by(dict(corr.loc[index]), by='value'))


def create_db_index():
    data_info = get_mongo_table(database='govstats', collection='data_info')
    data_info.create_index([("code", 1), ("time", 1)], unique=True, background=True)


if __name__ == '__main__':
    # find_mata_data()
    # find_all_data()
    #handle_gov_jd_data()
    # find_mata_data()
    handle_gov_yd_data()
    # energy_cov_data()
    # find_data()
