import akshare as ak
import google.generativeai as genai
from datetime import datetime
from big_models.google_api import *
from utils.tool import load_json_data
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
from utils.actions import show_data
import matplotlib.pyplot as plt
# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
from utils.actions import try_get_action
warnings.filterwarnings('ignore')
def big_model_macro_data():
    def handel_summary_data(output_txt):
        return output_txt.replace("\n", "").replace("*", "").replace(" ", "").replace("-", "")

    #宏观总结
    macro_summary_dict = {}

    #加载模型
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    genai.configure(api_key=api_key, transport='rest')
    model = genai.GenerativeModel(version)

    # 获取最新存款准备金数据
    macro_china_reserve_requirement_ratio_df = try_get_action(ak.macro_china_reserve_requirement_ratio, try_count=3)
    if macro_china_reserve_requirement_ratio_df is not None:
        macro_china_reserve_requirement_ratio_df.sort_values(by='生效时间', inplace=True, ascending=False)
        data = macro_china_reserve_requirement_ratio_df.head(1)
        back_remark = data['备注'].values[0]
        macro_summary_dict['中国存款准备金率'] = back_remark
    else:
        print("获取最新中国存款准备金率失败")

    # 中国宏观杠杆率
    macro_cnbs_df = try_get_action(ak.macro_cnbs,try_count=3)
    if macro_cnbs_df is not None:
        macro_cnbs_df.sort_values(by='年份', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月各部门宏观杠杆率数据，请总结分析，表格:""" + handle_model_table_data(
            macro_cnbs_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国宏观杠杆率'] = out_txt
        else:
            print("中国宏观杠杆率模型获取总结失败")
    else:
        print("获取中国宏观杠杆率失败")

    # 企业商品价格指数
    macro_china_qyspjg_df = try_get_action(ak.macro_china_qyspjg,try_count=3)
    if macro_china_qyspjg_df is not None:
        macro_china_qyspjg_df.sort_values(by='月份', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月企业商品价格指数数据，请总结分析，表格:""" + handle_model_table_data(
            macro_china_qyspjg_df.head(10))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国企业商品价格指数'] = out_txt
        else:
            print("企业商品价格指数模型获取总结失败")
    else:
        print("获取企业商品价格指数失败")

    # 外商直接投资数据
    macro_china_fdi_df = try_get_action(ak.macro_china_fdi,try_count=3)
    if macro_china_fdi_df is not None:
        macro_china_fdi_df.sort_values(by='月份', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月外商直接投资数据，请总结分析，表格:""" + handle_model_table_data(
            macro_china_fdi_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国外商直接投资'] = out_txt
        else:
            print("中国外商直接投资模型获取总结失败")
    else:
        print("外商直接投资数据失败")

    # LPR品种数据
    macro_china_lpr_df = try_get_action(ak.macro_china_lpr,try_count=3)
    if macro_china_lpr_df is not None:
        macro_china_lpr_df.sort_values(by='TRADE_DATE', inplace=True, ascending=False)
        macro_china_lpr_df.rename(columns={"TRADE_DATE": "时间", "LPR1Y": "1年LPR", "LPR5Y": "5年LPR"}, inplace=True)
        macro_china_lpr_df = macro_china_lpr_df[['时间', '1年LPR', '5年LPR']]
        input_str = """给定表格是中国最近几个月LPR品种数据，请总结分析，表格:""" + handle_model_table_data(
            macro_china_lpr_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国LPR品种'] = out_txt
        else:
            print("中国LPR品种，模型获取总结失败")
    else:
        print("获取LPR品种数据失败")

    # 社会融资规模增量统计
    macro_china_shrzgm_df = try_get_action(ak.macro_china_shrzgm,try_count=3)
    if macro_china_shrzgm_df is not None:
        macro_china_shrzgm_df.sort_values(by='月份', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月社会融资规模增量统计数据，请总结分析，表格:""" + handle_model_table_data(
            macro_china_shrzgm_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['社会融资规模增量'] = out_txt
        else:
            print("社会融资规模增量统计，模型获取总结失败")
    else:
        print("获取社会融资规模增量统计失败")

    # 中国CPI年率报告
    macro_china_cpi_yearly_df = try_get_action(ak.macro_china_cpi_yearly,try_count=3)
    if macro_china_cpi_yearly_df is not None:
        macro_china_cpi_yearly_df.rename(columns={"date": "日期", "value": "年率"}, inplace=True)
        macro_china_cpi_yearly_df.sort_values(by='日期', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月CPI年率报告，请总结分析，表格:""" + handle_model_table_data(
            macro_china_cpi_yearly_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国CPI年率'] = out_txt
        else:
            print("中国CPI年率，模型获取总结失败")
    else:
        print("获取中国CPI年率失败")

    # 中国CPI月率报告
    macro_china_cpi_monthly_df = try_get_action(ak.macro_china_cpi_monthly,try_count=3)
    if macro_china_cpi_monthly_df is not None:
        macro_china_cpi_monthly_df = pd.DataFrame(data=macro_china_cpi_monthly_df.values,
                                                  index=macro_china_cpi_monthly_df.index, columns=['月率'])
        macro_china_cpi_monthly_df['时间'] = macro_china_cpi_monthly_df.index
        macro_china_cpi_monthly_df.sort_values(by='时间', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月CPI月率报告，请总结分析，表格:""" + handle_model_table_data(
            macro_china_cpi_monthly_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国CPI月率'] = out_txt
        else:
            print("中国CPI月率，模型获取总结失败")
    else:
        print("获取中国CPI月率失败")

    # 中国PPI年率报告
    macro_china_ppi_yearly_df = try_get_action(ak.macro_china_ppi_yearly,try_count=3)
    if macro_china_ppi_yearly_df is not None:
        macro_china_ppi_yearly_df = pd.DataFrame(data=macro_china_ppi_yearly_df.values,
                                                 index=macro_china_ppi_yearly_df.index, columns=['年率'])
        macro_china_ppi_yearly_df['时间'] = macro_china_ppi_yearly_df.index
        macro_china_ppi_yearly_df.sort_values(by='时间', inplace=True, ascending=False)
        input_str = """给定表格是中国最近几个月PPI年率报告，请总结分析，表格:""" + handle_model_table_data(
            macro_china_ppi_yearly_df.head(12))
        out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                                 is_ret_json=False)
        if out_txt is not None:
            out_txt = handel_summary_data(out_txt)
            macro_summary_dict['中国PPI年率'] = out_txt
        else:
            print("中国PPI年率，模型获取总结失败")
    else:
        print("获取中国PPI年率失败")

    data = pd.DataFrame(data=[macro_summary_dict])
    input_str = """给定表格是中国最近宏观指标概括，给出当前宏观环境情绪分类[积极，悲观，中性]，并给出理由，表格：""" + handle_model_table_data(
        data)
    print(input_str)
    out_txt = try_get_action(simple_big_gen_model_fn, try_count=3, model=model, request_txt=input_str,
                             is_ret_json=False)
    if out_txt is not None:
        lines = out_txt.split("\n")
        first = lines[0]
        macro_cls = None
        for cl in ['积极', '悲观', '中性']:
            if cl in first:
                macro_cls = cl
                break
        out_txt = "".join(lines[2:]).replace("理由：", "")
        reason = handel_summary_data(out_txt)
        print(macro_cls,reason)
        macro_summary_dict['宏观情绪'] = macro_cls
        macro_summary_dict['宏观情绪原因'] = reason
        return macro_summary_dict
    return None

def enter_big_model_analysis_macro():
    summary = big_model_macro_data()
    month = datetime.now().strftime("%Y%m01")
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    update_request = []
    if summary is not None:
        new_dict = {"data_type": "macro_summary", "sentiment": summary['宏观情绪'],"reason":summary['宏观情绪原因'],
                    "time": month, "code": "big_model_summary_macro"}
        update_request.append(
            UpdateOne({"code": new_dict['code'], 'time': new_dict['time'], "data_type": new_dict['data_type']},
                      {"$set": new_dict},
                      upsert=True)
        )
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()

def global_pmi_data_analysis():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    condition = {"data_type": "global_micro_data", "metric_code": {"$regex": "PMI"}, "time": {"$gt": "2017-01-01"}}
    gloabel_pmi_gt_and_lt_count = {}
    for ele in stock_common.find(condition, projection={"_id": False}).sort("time"):
        time = ele['time']
        pub_value = float(ele['pub_value'])
        pub_month  = time[0:7]
        gloabel_pmi_gt_and_lt_count.setdefault(pub_month,{"lt":0,"gt":0})
        if pub_value>50:
            gloabel_pmi_gt_and_lt_count[pub_month]['gt'] += 1
        else:
            gloabel_pmi_gt_and_lt_count[pub_month]['lt'] += 1

    new_datas = []
    for time,lt_gt in gloabel_pmi_gt_and_lt_count.items():
        rate = round(lt_gt['lt']/lt_gt['gt'],4)
        new_datas.append({"time":time,"lt_and_gt_rate":rate,"lt":lt_gt['lt'],"gt":lt_gt['gt']})
    pd_data = pd.DataFrame(new_datas)
    pd_data.set_index(keys='time',inplace=True)
    show_data(pd_data)
    pd_data['lt_and_gt_rate'].plot(kind='line', title='全球公布的pmi小于50和大于50的个数比值', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()
    pd_data[['lt','gt']].plot(kind='line', title='全球公布的pmi小于50和大于50的个数', rot=45, figsize=(15, 8),
                                   fontsize=10)
    plt.show()




if __name__ == '__main__':
    enter_big_model_analysis_macro()
