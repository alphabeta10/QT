import os
from utils.tool import get_data_from_mongo
from utils.tool import load_json_data

key_json_data = load_json_data('model_key.json')
os.environ['DEEPSEEK_API_KEY'] = key_json_data.get('deepseek_key',None)  # 临时方法，更推荐使用`.env`文件
if "GOOGLE_API_KEY" not in os.environ:
    os.environ["GOOGLE_API_KEY"] = key_json_data.get("google_key",None)


from langchain_google_genai import ChatGoogleGenerativeAI
from jinja2 import Environment, FileSystemLoader, select_autoescape
from langchain_deepseek import ChatDeepSeek
from datetime import datetime,timedelta
from big_models.google_api import handle_model_table_data

from pydantic import BaseModel,Field
from typing import List,Optional


class RiskWaring(BaseModel):
    category:str = Field(...,description='风险的类别')
    details:str = Field(...,description='风险详情')
    score:float = Field(...,description='风险分数最低分0，最高分100分')

class MacroPolicy(BaseModel):
    monetary_policy:str = Field(...,description='货币政策')
    fiscal_policy:str = Field(...,description='财政政策')
    industry_policy:str = Field(...,description='行业政策')

class RecommendedFocus(BaseModel):
    category:str = Field(...,description='类别')
    targets:List[str] = Field(...,description='目标标的')
    rationale:str = Field(...,description='推理详情')



class MacroResult(BaseModel):
    risk_warning:List[RiskWaring]
    macro_policy:MacroPolicy
    recommended_focus:List[RecommendedFocus]



def get_model(name='deepseek'):
    if name=='deepseek':
        llm = ChatDeepSeek(base_url='https://api.deepseek.com/v1', model='deepseek-reasoner',
                         api_key=os.getenv("DEEPSEEK_API_KEY"))
    elif name=='google':
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            temperature=0,
            max_tokens=None,
            timeout=None,
            max_retries=2,
            # other params...
            transport='rest',
        )
    else:
        llm = None
    return llm

def global_new_analysis_node(start_date, end_date, model_name='google'):
    env = Environment(
        loader=FileSystemLoader(os.path.dirname(__file__)),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    prompt_name = 'macro_top'
    template = env.get_template(f"{prompt_name}.md")
    state_vars = {}
    system_promote = template.render(**state_vars)

    projection = {'_id': False}
    sort_key = "time"
    database = "stock"
    collection = "all_news"
    condition = {"time": {"$gte": start_date,"$lte":end_date}}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['title','content','time']
    if data is not None and len(data)>0:
        ret = handle_model_table_data(data[cols])
        print(len(ret))
        messages = [{"role": "system", "content": system_promote}] + [
            {"role": "user", "content": ret}]
        model = get_model(name=model_name)
        model = model.with_structured_output(MacroResult)
        ret = model.invoke(messages)
        if isinstance(ret,MacroResult):
            json_str = ret.model_dump_json()
            print(json_str)
        else:
            print(ret)


def generator_day_llm_analysis_global_news():
    now = datetime.now()
    before_day = 7
    day_list = []
    for i in range(before_day):
        before_day = now - timedelta(i)
        start_date_str = before_day.strftime("%Y-%m-%d 00:00:00")
        end_date_str = before_day.strftime("%Y-%m-%d 23:59:59")
        day_list.append([start_date_str, end_date_str])
    for ele_date in day_list:
        start_date, end_date = ele_date
        print(start_date, end_date)
        global_new_analysis_node(start_date, end_date,model_name='deepseek')






if __name__ == '__main__':
    generator_day_llm_analysis_global_news()