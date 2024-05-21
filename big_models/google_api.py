import pandas as pd
import json


def handle_model_table_data(pd_data: pd.DataFrame):
    """
    生成要分析的表格数据喂给生成模型
    :param pd_data:
    :return:
    """
    cols = pd_data.columns
    split_header = ['---'] * len(cols)
    split_header = "|" + "|".join(split_header) + "|"
    header = "| " + " | ".join(cols) + " |"
    input_table_str = header + "\n" + split_header + "\n"
    for index in pd_data.index:
        row_dict = dict(pd_data.loc[index])
        ele_list = []
        for col in cols:
            ele = str(row_dict[col])
            if ele == 'nan':
                ele = ''
            if ele.replace(" ","")!="":
                ele_list.append(ele)
        row_ele = "| " + " | ".join(ele_list) + " |"
        input_table_str += row_ele + "\n"
    return input_table_str


def simple_big_gen_model_fn(model,request_txt:str,is_ret_json=True):
    safety_settings = [
        {
            "category": "HARM_CATEGORY_HARASSMENT",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_HATE_SPEECH",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        }
    ]
    response = model.generate_content(request_txt, safety_settings=safety_settings)
    if is_ret_json is True:
        import re
        match = re.search(r"\{(.+)\}", response.text)
        if match:
            json_str = match.group(0)
            data = json.loads(json_str)
            return data
        else:
            return None
    else:
        return response.text


def google_big_gen_model_comm_fn(data_df: pd.DataFrame, model, request_txt:str):
    """
    google模型，返回是json格式数据
    :param data_df:
    :return:
    """
    input_str = handle_model_table_data(data_df)
    request_txt = request_txt.replace("${input_str}",input_str)
    safety_settings = [
        {
            "category": "HARM_CATEGORY_HARASSMENT",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_HATE_SPEECH",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        }
    ]
    response = model.generate_content(request_txt, safety_settings=safety_settings)
    try:
        json_data = json.loads(response.text)
        return json_data
    except Exception as e:
        print(e)
        print(request_txt)
        print(response.text)
    return None



def comm_google_big_gen_model(data_df: pd.DataFrame, model, demo_input=None, demo_output=None,contain_keys=None):
    """
    google模型情感分析
    :param data_df:
    :return:
    """
    input_str = handle_model_table_data(data_df)
    if demo_input is None:
        demo_input = """| 发布时间 | 新闻内容 |\n|---|---|\n| 2024-01-23 21:30:54 | 已使用资金总额约为9.17亿元。 2023年1至6月份，东方财富的营业收入构成为：证券业占比62.78%，信息技术服务业占比37.18%。 东方财富的董事长是其实，男，54岁，学历背景为博士；总经理是郑立坤，男，40岁，学历背景为本科。 截至发稿，东方财富市值为2015亿元。 |"""
    if demo_output is None:
        demo_output = """[{"时间":"2024-01-23 21:30:54","内容":"已使用资金总额约为9.17亿元。 2023年1至6月份，东方财富的营业收入构成为：证券业占比62.78%，信息技术服务业占比37.18%。 东方财富的董事长是其实，男，54岁，学历背景为博士；总经理是郑立坤，男，40岁，学历背景为本科。 截至发稿，东方财富市值为2015亿元。","情感类别":"中性","摘要":"东方财富市值为2015亿元"}]"""

    request_txt = f"给定表格中的新闻内容情感分类为[积极，中性，悲观]以及新闻内容摘要提取。输入：{demo_input} \n 输出：{demo_output} \n 输入：{input_str} 输出："
    safety_settings = [
        {
            "category": "HARM_CATEGORY_HARASSMENT",
            "threshold": "BLOCK_NONE"
        },
        {
            "category": "HARM_CATEGORY_HATE_SPEECH",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        },
        {
            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
            "threshold": "BLOCK_MEDIUM_AND_ABOVE"
        }
    ]
    response = model.generate_content(request_txt, safety_settings=safety_settings)
    try:
        json_data = json.loads(response.text)
        if isinstance(json_data,list) and len(json_data)>0:
            for key in contain_keys:
                if key not in json_data[0].keys():
                    return None
        if isinstance(json_data,dict):
            for key in contain_keys:
                if key not in json_data.keys():
                    return None
        return json_data
    except Exception as e:
        print(e)
        print(response.text)
        print(request_txt)
    return None