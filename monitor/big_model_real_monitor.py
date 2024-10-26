import json
import uuid
from datetime import datetime,timedelta
import requests

from utils.tool import load_json_data,mongo_bulk_write_data,get_data_from_mongo
from utils.actions import try_get_action
from data.comm_real_news_data import get_all_detail_data,get_100ppi_detail_new_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm

class BasicMonitor(object):

    def __init__(self,**kwargs):
        self.names = kwargs.get("names",[])


    def __data_to_mongo(self, datas):
        monitor_data_col = get_mongo_table(database='stock', collection="monitor_data")
        update_request = []
        for data in datas:
            update_request.append(
                UpdateOne({"uuid":data.get("uuid")},
                          {"$set": data},
                          upsert=True))
            if len(update_request)>100:
                mongo_bulk_write_data(monitor_data_col,update_request)
                update_request.clear()
        if len(update_request)>0:
            mongo_bulk_write_data(monitor_data_col,update_request)

    def __load_uuid(self):
        before30day = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        database = 'stock'
        collection = 'monitor_data'
        condition = {"name":{"$in":self.names},"time":{"$gt":before30day}}
        projection = {"_id":False,"uuid":True}
        sort_key = 'uuid'
        df = get_data_from_mongo(database=database,collection=collection,condition=condition,projection=projection,sort_key=sort_key)
        if not df.empty:
            return list(df['uuid'].values)
        return []




    def __get_baidu_access_token(self, file_name=None):
        """
        使用 API Key，Secret Key 获取access_token，替换下列示例中的应用API Key、应用Secret Key
        """
        file_name = file_name if file_name else 'baidu_bigmodel_key.json'
        combine_key_dict = load_json_data(file_name)
        secret_key = combine_key_dict.get("secret_key", None)
        api_key = combine_key_dict.get("api_key", None)
        if not secret_key or not api_key:
            print("加载 api key 失败！！！")
            raise Exception("加载 api key 失败！！！")

        url = f"https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id={api_key}&client_secret={secret_key}"
        payload = json.dumps("")
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json().get("access_token")

    def __baidu_model_request(self, user_request_data, check_keys):
        """
        百度模型
        :return:
        """
        url = "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/ernie-speed-128k?access_token=" + self.__get_baidu_access_token()

        payload = json.dumps({
            "messages": [user_request_data]
        })
        headers = {
            'Content-Type': 'application/json'
        }

        def inner_request():
            response = requests.request("POST", url, headers=headers, data=payload)
            res = json.loads(response.text)['result'].replace("```json", "").replace("```", "").replace("\n",
                                                                                                        "").replace(" ",
                                                                                                                    "")
            ret_json = json.loads(res)
            if isinstance(ret_json,list):
                for ele in ret_json:
                    for ck in check_keys:
                        if not ele.get(ck):
                            return None
            if isinstance(ret_json,dict):
                for ck in check_keys:
                    if not ret_json.get(ck):
                        return None
            return ret_json

        ret = try_get_action(inner_request,try_count=3)
        return ret



    def get_news(self):
        """
        获取新闻数据
        :return:
        """
        data = get_all_detail_data(self.names)
        new_detail_data = {}
        print("start load uuid")
        uuid_list = self.__load_uuid()
        print("end load uuid")
        print(f"uuid len = {len(uuid_list)}")
        for key, values in data.items():
            new_detail_data.setdefault(key, [])
            for ele in values:
                url = ele['url']
                uuid_str = str(uuid.uuid5(uuid.NAMESPACE_DNS, ele['time'] + ele['header']))
                if uuid_str not in uuid_list:
                    news = get_100ppi_detail_new_data(url)
                    sps = news.split("\u3000\u3000(文章来源：生意社)")
                    if len(sps)>0:
                        news = sps[0].strip().replace("\r","")
                    ele['detail_news'] = news
                    ele['uuid'] = uuid_str
                    new_detail_data[key].append(ele)
        return new_detail_data

    def handle_model_analysis(self):
        new_detail_data = self.get_news()
        datas = []
        for key,values in new_detail_data.items():
            print(f"{key} len {len(values)}")
            for ele in tqdm(values):
                news = ele['detail_news']
                demo_str = f'<article>{news}</article>'
                user_request_data = {
                    "role": "user",
                    "content": "你将收到多段新闻用XML标签分割。首先概括新闻的摘要，给出情感分类为[积极，中性，悲观]，给出新闻类别，给出新闻涉及的国家,返回json格式[{\"摘要\":\"新闻的摘要\",\"情感类别\":\"新闻的情感类别\",\"新闻类别\":\"新闻的类别\",\"涉及国家\":\"新闻涉及的国家\"}]。新闻如下：" + demo_str
                }
                check_keys = ['摘要', '情感类别', '新闻类别', '涉及国家']
                ret_json = self.__baidu_model_request(user_request_data, check_keys)
                if isinstance(ret_json,list):
                    for json_ele in ret_json:
                        for k,v in json_ele.items():
                            ele[k] = v
                if isinstance(ret_json,dict):
                    for k,v in ret_json.items():
                        ele[k] = v
                datas.append(ele)
        self.__data_to_mongo(datas)
    def sender(self):
        """
        统计发送邮件
        :return:
        """
        pass


if __name__ == '__main__':
    names = ['WTI原油', 'Brent原油','玻璃']
    basic = BasicMonitor(names=names)
    basic.handle_model_analysis()



