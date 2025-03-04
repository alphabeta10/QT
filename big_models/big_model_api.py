from google import genai
import json
from utils.tool import load_json_data
from openai import OpenAI
import httpx
from google.genai import types


def google_model_client():
    """
    client模式的google api
    :return:
    """
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    client = genai.Client(api_key=api_key)
    return client,version



def extract_dis_relation_data(url=None,is_ret_json=False):
    client, version = google_model_client()
    doc_data = httpx.get(url).content
    prompt = '''文档是公司投资者关系活动记录表，请提取该文档中的时间，地点，以及问题和对答的摘要。
        用json格式返回：
        Result = {'time':str,'location':str,'QA':dict}
        Return:Result'''
    response = client.models.generate_content(
        model=version,
        contents=[
            types.Part.from_bytes(
                data=doc_data,
                mime_type='application/pdf',
            ),
            prompt])

    if is_ret_json is True:
        import re
        match = re.search(r"\{(.+)\}", response.text)
        if match:
            json_str = match.group(0)
            data = json.loads(json_str)
            return data
        else:
            json_str = response.text.replace("```json","").replace("```","")
            data = json.loads(json_str)
            return data
    else:
        return response.text


def google_simple_model_fn(client,prompt:str,is_ret_json=True):
    """
    google请求数据的模式
    :param client:
    :param prompt:
    :param is_ret_json:
    :return:
    """
    response = client.models.generate_content(
        model='gemini-2.0-flash',
        contents=prompt,
    )
    if is_ret_json is True:
        import re
        match = re.search(r"\{(.+)\}", response.text)
        if match:
            json_str = match.group(0)
            data = json.loads(json_str)
            return data
        else:
            json_str = response.text.replace("```json","").replace("```","")
            data = json.loads(json_str)
            return data
    else:
        return response.text


def deepseek_client():
    """
    deepseek 客户端
    :return:
    """
    api_key_json = load_json_data("deepseek_api.json")
    api_key = api_key_json['api_key']
    base_url = api_key_json['base_url']
    client = OpenAI(api_key=api_key, base_url=base_url)
    return client

def deepseek_simple_model_fn(client,messages:list,stream=False):
    response = client.chat.completions.create(
        model="deepseek-reasoner",
        messages=messages,
        stream=stream
    )
    return response.choices[0].message.content