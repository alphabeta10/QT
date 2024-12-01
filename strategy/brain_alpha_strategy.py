import pandas as pd
from requests.auth import HTTPBasicAuth
import json
import requests
import numpy as np
from utils.tool import load_json_data
import time
import os
import logging

class BrainFastExp(object):
    def __init__(self, brain_credential_file, **kwargs):
        if not brain_credential_file:
            raise Exception("brain credential file can't none")
        json_data = load_json_data(brain_credential_file)
        self.username, self.password = json_data['username'], json_data['password']

    def sign_in(self):
        sess = requests.Session()
        sess.auth = HTTPBasicAuth(self.username, self.password)
        response = sess.post("https://api.worldquantbrain.com/authentication")
        print("log in response ", response.json())
        return sess

    def get_data_fields(self, s, searchScope, dataset_id='', search=''):
        instrument_type = searchScope['instrumentType']
        region = searchScope['region']
        delay = searchScope['delay']
        universe = searchScope['universe']
        if len(search) == 0:
            url_template = "https://api.worldquantbrain.com/data-fields?" \
                           f"&instrumentType={instrument_type}" \
                           f"&region={region}&delay={str(delay)}" \
                           f"&universe={universe}&dataset.id={dataset_id}" \
                           "&limit=50&offset={x}"
            res = s.get(url_template.format(x=0)).json()
            count = res['count']
        else:
            url_template = "https://api.worldquantbrain.com/data-fields?" \
                           f"&instrumentType={instrument_type}" \
                           f"&region={region}&delay={str(delay)}" \
                           f"&universe={universe}&limit=50" \
                           f"&search={search}" \
                           "&offset={x}"
            count = 100
        datafields_list = []

        for x in range(0, count, 50):
            datafields = s.get(url_template.format(x=x))
            datafields_list.append(datafields.json()['results'])
        datafields_list_flat = [item for sublist in datafields_list for item in sublist]
        return pd.DataFrame(datafields_list_flat)
    def construct_fast_expression(self,**kwargs):
        pass

    def send_alpah_expression_to_brain(self,alpha_list,sess=None):
        alpha_fail_attempt_tolerance = 15
        if sess is None:
            sess = self.sign_in()
        for alpha in alpha_list:
            keep_trying = True
            failure_count = 0

            while keep_trying:
                try:
                    sim_resp = sess.post('https://api.worldquantbrain.com/simulations', json=alpha)
                    sim_progress_url = sim_resp.headers['location']
                    logging.info(f"Alpha location is :{sim_progress_url}")
                    print(f"Alpha location is :{sim_progress_url}")
                    keep_trying = False
                except Exception as e:
                    logging.error(f"No location,sleep 15 and retry,error msg:{e}")
                    time.sleep(15)
                    failure_count += 1
                    if failure_count>= alpha_fail_attempt_tolerance:
                        sess = self.sign_in()
                        failure_count = 0
                        logging.error(f"No location for too many times,move to next alpha{alpha['regular']}")
                        print(f"No location for too many times,move to next alpha{alpha['regular']}")
                        break





if __name__ == '__main__':

    temp_alpha = '''my_group = market;
my_group2 = bucket(rank(cap),range='0,1,0.1');
alpha=rank(group_rank(ts_decay_linear(volume/ts_sum(volume,252),10),my_group)*group_rank(ts_rank(vec_avg({field}),5),my_group)*group_rank(-ts_delta(close,5),my_group));
trade_when(volume>adv20,group_neutralize(alpha,my_group2),-1)'''
    """
    模版如上表达式，参数是P，N，rate0，rate1，filed分别表示如下： P和N表示天，必须是P>N,rate0和rate1是权重，rate0+rate1=1，filed是数据字段 ，可能是close ,volume
    该参数是变化的，调整该参数，alpha
    效果不一样,调整
    搜索空间260
    """


    alpha_List = []


    brain = BrainFastExp('brain.json')
    searchScope = {
        "region": "USA",
        "delay": "1",
        "universe": "TOP3000",
        "instrumentType": "EQUITY",
    }
    s = brain.sign_in()
    df = brain.get_data_fields(s,searchScope,dataset_id='fundamental6')
    fields = df[df['type']=='VECTOR']['id'].values

    for field in fields:
        alpha = temp_alpha.format(field=field)
        simulation_data = {
            "type": "REGULAR",
            "settings": {
                'instrumentType': "EQUITY",
                'region': "USA",
                'universe': "TOP3000",
                'delay': 1,
                'decay': 1,
                "neutralization": "NONE",
                'truncation': 0.01,
                'pasteurization': 'ON',
                'unitHandling': "VERIFY",
                'nanHandling': 'ON',
                'language': "FASTEXPR",
                'visualization': False,
            },
            "regular": alpha
        }
        alpha_List.append(simulation_data)
    brain.send_alpah_expression_to_brain(alpha_List,s)



