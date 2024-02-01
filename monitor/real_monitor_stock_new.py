from datetime import datetime,timedelta
import akshare as ak
import google.generativeai as genai
import pandas as pd
from big_models.google_api import comm_google_big_gen_model
from utils.send_msg import MailSender
from utils.tool import load_json_data
import schedule
import time
def stock_real_analysis_main():
    start_date_str = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')
    code_dict = {
        # 半导体 "other_keys": ["人工智能", "5G通信", '人形机器人', '发改委']
        "工信部": {"name": "工信部", "is_stock": False},
        "人工智能": {"name": "人工智能", "is_stock": False},
        "5G通信": {"name": "5G通信", "is_stock": False},
        "人形机器人": {"name": "人形机器人", "is_stock": False},
        "发改委": {"name": "发改委", "is_stock": False},
        "002409": {"name": "雅克科技", "other_keys": ["存储", "半导体"],"is_stock":True},
    }

    # batch request google api model
    no_stock_key_set = set()
    for code, combine_dict in code_dict.items():
        if combine_dict['is_stock'] is False:
            name = combine_dict['name']
            no_stock_key_set.add(name)
        [no_stock_key_set.add(e) for e in combine_dict.get("other_keys", [])]
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    # genai.configure(api_key=api_key,transport='rest')
    # model = genai.GenerativeModel('gemini-pro')

    col = ['发布时间', '新闻标题', '新闻内容']
    stock_telegraph_cls_df = ak.stock_telegraph_cls(symbol="全部")
    tel_col = ['标题', '内容', '发布日期', '发布时间']
    tel_mapping = {"标题": "新闻标题", "内容": "新闻内容"}
    stock_telegraph_cls_df = stock_telegraph_cls_df[tel_col]
    stock_telegraph_cls_df.rename(columns=tel_mapping, inplace=True)
    stock_telegraph_cls_df['发布时间'] = stock_telegraph_cls_df.apply(
        lambda row: str(row['发布日期']) + " " + str(row['发布时间']), axis=1)
    stock_telegraph_cls_df = stock_telegraph_cls_df[col]
    keys = "|".join(list(no_stock_key_set))
    filter_stock_telegraph_cls_df = stock_telegraph_cls_df[stock_telegraph_cls_df['新闻内容'].str.contains(keys)]

    html_msg = ""
    sender = MailSender()
    stock_df_list = []
    for code, combine_dict in code_dict.items():
        name = combine_dict['name']
        contain_list = [name]
        contain_list.extend(combine_dict.get("other_keys", []))
        if combine_dict['is_stock']:
            stock_new_df = ak.stock_news_em(symbol=code)
            stock_new_df = stock_new_df[col]
            stock_new_df = stock_new_df[stock_new_df['发布时间'] > start_date_str]
            if len(stock_new_df)>0:
                stock_df_list.append(stock_new_df)
    stock_df = None
    all_df_list = []
    if len(stock_df_list)>0:
        stock_df = pd.concat(stock_df_list)
        all_df_list.append(stock_df)
    if len(filter_stock_telegraph_cls_df)>0:
        all_df_list.append(filter_stock_telegraph_cls_df)
    if len(all_df_list)>0:
        all_df = pd.concat(all_df_list)
        print(all_df)
        # json_data = google_big_gen_model_sentence_analysis(all_df,model)
        # print(json_data)
    #         json_data = google_big_gen_model_sentence_analysis(stock_new_df,model)
    #         neg_data = [row for row in json_data if row['情感类别'] in ['中性','悲观']]
    #         pos_data = [row for row in json_data if row['情感类别'] in ['积极']]
    #
    #         pre_neg_val = neg_count_dict.get(code,0)
    #         pre_pos_val = pos_count_dict.get(code,0)
    #
    #         send_neg_data = []
    #         send_pos_data = []
    #         if pre_pos_val!=len(pos_data):
    #             send_pos_data = pos_data
    #             pos_count_dict[code] = len(pos_data)
    #         if pre_neg_val!=len(neg_data):
    #             send_neg_data = neg_data
    #             neg_count_dict[code] = len(neg_data)
    #         html_msg += construct_msg_data(name,send_pos_data,send_neg_data)
    # if html_msg!='':
    #     sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], "个股行业新闻数据情感分析结果邮件", html_msg)
    #     sender.close()
def construct_msg_data(name:str,pos_data:list,neg_data:list):
    mail_msg = ""
    if len(pos_data)>0:
        mail_msg += f"<p>{name}积极如下消息</p>"
        mail_msg += f"<table>"
        mail_msg += f"<tr><th>时间</th> <th>摘要</th><th>详细内容</th> </tr>"
        for combine_data in pos_data:
            time = combine_data['时间']
            mail_msg += f"<tr> <td>{time}</td> <td>{combine_data['摘要']}</td> <td>{combine_data['内容']}</td></tr>"
        mail_msg += "</table>"
    if len(neg_data)>0:
        mail_msg += f"<p>{name}中性和悲观如下消息</p>"
        mail_msg += f"<table>"
        mail_msg += f"<tr><th>时间</th> <th>摘要</th><th>详细内容</th><th>情感类别</th> </tr>"
        for combine_data in neg_data:
            time = combine_data['时间']
            mail_msg += f"<tr> <td>{time}</td> <td>{combine_data['摘要']}</td> <td>{combine_data['内容']}</td> <td>{combine_data['情感类别']}</td></tr>"
        mail_msg += "</table>"
    return mail_msg

if __name__ == '__main__':
    neg_count_dict = {}
    pos_count_dict = {}
    stock_real_analysis_main()
    # schedule.every(30).minutes.do(stock_real_analysis_main)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(10)