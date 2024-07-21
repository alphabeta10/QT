import os.path

import akshare as ak
import google.generativeai as genai
from datetime import datetime

import pandas as pd

from big_models.google_api import *
from utils.tool import load_json_data, get_data_from_mongo
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
from utils.actions import show_data
import matplotlib.pyplot as plt
from analysis.common_analysis import BasicAnalysis
from pyecharts.charts import Page, Tab
from analysis.analysis_tool import convert_pd_data_to_month_data

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
from utils.actions import try_get_action

warnings.filterwarnings('ignore')


def big_model_macro_data():
    def handel_summary_data(output_txt):
        return output_txt.replace("\n", "").replace("*", "").replace(" ", "").replace("-", "")

    # 宏观总结
    macro_summary_dict = {}

    # 加载模型
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
    macro_cnbs_df = try_get_action(ak.macro_cnbs, try_count=3)
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
    macro_china_qyspjg_df = try_get_action(ak.macro_china_qyspjg, try_count=3)
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
    macro_china_fdi_df = try_get_action(ak.macro_china_fdi, try_count=3)
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
    macro_china_lpr_df = try_get_action(ak.macro_china_lpr, try_count=3)
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
    macro_china_shrzgm_df = try_get_action(ak.macro_china_shrzgm, try_count=3)
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
    macro_china_cpi_yearly_df = try_get_action(ak.macro_china_cpi_yearly, try_count=3)
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
    macro_china_cpi_monthly_df = try_get_action(ak.macro_china_cpi_monthly, try_count=3)
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
    macro_china_ppi_yearly_df = try_get_action(ak.macro_china_ppi_yearly, try_count=3)
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
        print(macro_cls, reason)
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
        new_dict = {"data_type": "macro_summary", "sentiment": summary['宏观情绪'], "reason": summary['宏观情绪原因'],
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
        pub_month = time[0:7]
        gloabel_pmi_gt_and_lt_count.setdefault(pub_month, {"lt": 0, "gt": 0})
        if pub_value > 50:
            gloabel_pmi_gt_and_lt_count[pub_month]['gt'] += 1
        else:
            gloabel_pmi_gt_and_lt_count[pub_month]['lt'] += 1

    new_datas = []
    for time, lt_gt in gloabel_pmi_gt_and_lt_count.items():
        rate = round(lt_gt['lt'] / lt_gt['gt'], 4)
        new_datas.append({"time": time, "lt_and_gt_rate": rate, "lt": lt_gt['lt'], "gt": lt_gt['gt']})
    pd_data = pd.DataFrame(new_datas)
    pd_data.set_index(keys='time', inplace=True)
    show_data(pd_data)
    pd_data['lt_and_gt_rate'].plot(kind='line', title='全球公布的pmi小于50和大于50的个数比值', rot=45, figsize=(15, 8),
                                   fontsize=10)
    plt.show()
    pd_data[['lt', 'gt']].plot(kind='line', title='全球公布的pmi小于50和大于50的个数', rot=45, figsize=(15, 8),
                               fontsize=10)
    plt.show()


def plot_bar_value_line_same_rate_data(x, y, same_y, y_label, same_y_label, x_label, title):
    plt.figure(figsize=(12, 6))
    # 绘柱状图
    plt.bar(x=x, height=y, label=y_label, color='Coral', alpha=0.8)
    # 在左侧显示图例
    plt.legend(loc="upper left")

    # 设置标题
    plt.title(title)
    # 为两条坐标轴设置名称
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    # 旋转45度
    plt.xticks(rotation=45)

    # 画折线图
    ax2 = plt.twinx()
    ax2.set_ylabel(same_y_label)
    # 设置坐标轴范围
    # ax2.set_ylim([-, 1.05])
    plt.plot(x, same_y, "r", marker='.', c='r', ms=5, linewidth='1', label=same_y_label)
    # 显示数字
    for a, b in zip(x, same_y):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=8)
    # 在右侧显示图例
    plt.legend(loc="upper right")
    plt.axhline(y=0)
    plt.show()


def add_pct(data: pd.DataFrame, data_key):
    data[data_key] = data[data_key].astype(float)
    data[f'{data_key}_pct_1'] = round(data[data_key].pct_change(1), 4)
    data[f'{data_key}_pct_2'] = round(data[data_key].pct_change(2), 4)
    data[f'{data_key}_pct_3'] = round(data[data_key].pct_change(3), 4)


def cn_traffic_data_analysis():
    """
    中国交通分析
    :return:
    """
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    before_year = datetime.now().year - 1
    start_time = f"{before_year}0101"
    if "20230401" > start_time:
        start_time = "20230401"
    for ele in news.find({"data_type": "traffic", "metric_code": "traffic", "time": {"$gt": start_time}},
                         projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data

    traffic_mapping_dict = {
        "tl_traffic": "铁路运输(万吨)",
        "gk_traffic": "港口吞吐量(万吨)",
        "gk_teu_traffic": "港口集装箱吞吐量(万标箱)",
        "gs_traffic": "货车通行(万辆)",
        "lj_traffic": "邮政揽件",
        "td_traffic": "邮政投递",
    }
    convert_type_col = list(traffic_mapping_dict.keys())
    for col in convert_type_col:
        data[[col]] = data[[col]].astype(float)
    data.set_index(keys=['time'], inplace=True)
    data = data[convert_type_col]
    for k in traffic_mapping_dict.keys():
        add_pct(data, k)
    for k, k_name in traffic_mapping_dict.items():
        plot_bar_value_line_same_rate_data(data.index.values, data[k].values, data[f'{k}_pct_1'].values, k_name,
                                           f'{k_name}环比比', '时间', f'{k_name}分析')


def wci_index_data_analysis():
    """
    集装箱指数分析
    :return:
    """
    wci_index_mapping_dict = {"composite": "综合集装箱指数", "shanghai-rotterdam": "上海-鹿特丹集装箱指数",
                              "rotterdam-shanghai": "鹿特丹-上海集装箱指数",
                              "shanghai-los angeles": "上海-洛杉矶集装箱指数",
                              "los angeles-shanghai": "洛杉矶-上海集装箱指数",
                              "shanghai-genoa": "上海-热那亚集装箱指数", "new york-rotterdam": "纽约-鹿特丹集装箱指数",
                              "rotterdam-new york": "鹿特丹-纽约集装箱指数"}

    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    before_year = datetime.now().year - 1

    for ele in news.find({"data_type": "wci_index", "time": {"$gt": f"{before_year}-01-01"}},
                         projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    for metric_code, name in wci_index_mapping_dict.items():
        data = pd_data[pd_data['metric_code'] == metric_code]
        data.set_index(keys=['time'], inplace=True)
        add_pct(data, 'wci')
        plot_bar_value_line_same_rate_data(data.index.values, data['wci'].values, data[f'wci_pct_1'].values, name,
                                           f'{name}环比比', '时间', f'{name}分析')


def cn_wci_index_data_analysis():
    """
    集装箱指数分析
    :return:
    """
    wci_index_mapping_dict = {"综合指数": "综合指数", "欧洲航线": "欧洲航线",
                              "美西航线": "美西航线"}

    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    before_year = datetime.now().year - 1

    for ele in news.find({"data_type": "cn_wci_index", "time": {"$gt": f"{before_year}0101"}},
                         projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    for metric_code, name in wci_index_mapping_dict.items():
        data = pd_data[pd_data['metric_code'] == metric_code]
        data.set_index(keys=['time'], inplace=True)
        add_pct(data, 'cur_month_data')
        plot_bar_value_line_same_rate_data(data.index.values, data['cur_month_data'].values,
                                           data[f'cur_month_data_pct_1'].values, name,
                                           f'{name}环比比', '时间', f'{name}分析')


def analysis_us_debt():
    condition = {"data_name": "Total Public Debt Outstanding", "data_time": {"$gt": '2001-01-01'}}
    database = 'stock'
    collection = 'micro'
    projection = {'_id': False}
    sort_key = "data_time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    show_data(data)
    data[['value']] = data[['value']].astype(float) / 1e6
    data.set_index(keys='data_time', inplace=True)
    data.sort_index(inplace=True)
    data['value'].plot(kind='line', title='美国债务（万亿）', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def analysis_us_m0():
    condition = {"data_name": "M0", "data_time": {"$gt": '2001-01-01'}}
    database = 'stock'
    collection = 'micro'
    projection = {'_id': False}
    sort_key = "data_time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    show_data(data)
    data[['value']] = data[['value']].astype(float) / 1e6
    data.set_index(keys='data_time', inplace=True)
    data.sort_index(inplace=True)
    data['value'].plot(kind='line', title='美国m0（万亿）', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def cn_pmi_analysis():
    code_dict = {'A0B0101_yd': '制造业采购经理指数', 'A0B0102_yd': '生产指数', 'A0B0103_yd': '新订单指数',
                 'A0B0104_yd': '新出口订单指数', 'A0B0105_yd': '在手订单指数', 'A0B0106_yd': '产成品库存指数',
                 'A0B0107_yd': '采购量指数', 'A0B0108_yd': '进口指数', 'A0B0109_yd': '出厂价格指数',
                 'A0B010A_yd': '主要原材料购进价格指数', 'A0B010B_yd': '原材料库存指数', 'A0B010C_yd': '从业人员指数',
                 'A0B010D_yd': '供应商配送时间指数', 'A0B010E_yd': '生产经营活动预期指数'}

    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}

    time = "201801"
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    """
    产成品库存指数,原材料库存指数,结论
    up,up,主动补库存预期未来需求向好
    down,down,去库存周期，降价处理自己仓库里的库存
    down,up,需求向好，被动去库存
    up,down,需求不好，
    """

    inventory_dict = {
        'A0B0106_yd': {"up": 0, "down": 1},
        'A0B010B_yd': {"up": 0, "down": 1},
    }
    inventory_conclude_dict = {
        "00": "主动补库存预期未来需求向好",
        "01": "需求不好",
        "10": "需求向好，被动去库存",
        "11": "去库存周期，降价处理自己仓库里的库存",
    }
    """
    采购量指数,结论
    up,需求上升
    down,需求放缓
    """
    purchase_dict = {'A0B0107_yd': {"up": "需求上升", "down": "需求放缓"}}

    """
    生产指数,结论
    up,供给增大
    down,供给放缓
    """
    product_dict = {
        'A0B0102_yd': {"up": "供给增大", "down": "供给放缓"}
    }

    """
    新订单指数,新出口订单指数,在手订单指数,结论
    up，内需好
    up,外需好
    """
    need_dict = {
        'A0B0103_yd': {"up": "内需向好", "down": "内需放缓"},
        'A0B0104_yd': {"up": "外需向好", "down": "外需放缓"},
        'A0B0105_yd': {"up": "订单完成进度慢", "down": "订单完成进度快"},
    }

    class_conclude_mapping = {
        "需求": need_dict,
        "供给": product_dict,
        "采购量": purchase_dict,
        "存货": inventory_dict,
    }
    new_data = pd.pivot_table(data, index='time', columns='code', values='data')
    diff_new_data = new_data.diff()
    result_data = {}
    final_concludes = []
    for index in diff_new_data.index:
        dict_data = dict(diff_new_data.loc[index])
        result_data.setdefault(index, {})
        final_conclude = ""
        for name, config_dict in class_conclude_mapping.items():
            concludes = []
            if name != '存货':
                for k, conclude_dict in config_dict.items():
                    v = dict_data.get(k, None)
                    if v is not None and v >= 0:
                        res = "up"
                    else:
                        res = "down"
                    conclude = conclude_dict[res]
                    result_data[index].setdefault(name, [])
                    result_data[index][name].append(conclude)
                    concludes.append(f"{conclude}")
            else:
                conclude = ""
                for k, conclude_dict in config_dict.items():
                    v = dict_data.get(k, None)
                    if v is not None and v >= 0:
                        res = "up"
                    else:
                        res = "down"
                    conclude += str(conclude_dict[res])
                conclude = inventory_conclude_dict[conclude]
                concludes.append(f"{conclude}")
                result_data[index].setdefault(name, [])
                result_data[index][name].append(conclude)
            final_conclude += name + "[" + ";".join(concludes) + "]"
        final_concludes.append({"time": index, "conclude": final_conclude})
    conclude_df = pd.DataFrame(data=final_concludes)
    new_data.reset_index(inplace=True)
    new_data = pd.merge(new_data, conclude_df, on=['time'], how='left')
    return new_data


class CNMacroAnalysis(BasicAnalysis):
    def __init__(self, *args, **kwargs):
        self.name = kwargs.get('name', 'default')
        self.analysis_dir = kwargs.get("dir", "gdp")
        if not os.path.exists(self.analysis_dir):
            os.mkdir(self.analysis_dir)

    def gdp_bar_line_tab_data(self, code_dict: dict, time='201001', file_name='default'):
        data = self.get_data_from_cn_st(code_dict, time=time)
        for col in code_dict.keys():
            data[f'{col}_same'] = round(data[col].pct_change(4) * 100, 2)
        tab = Tab()
        for code, name in code_dict.items():
            bar = self.bar_line_overlap(list(data['time'].values), {f"{name}(亿元)": list(data[code].values)},
                                        {f"{name}同比(%)": list(data[f'{code}_same'].values)})
            tab.add(bar, name)
        tab.render(f"{self.analysis_dir}/{file_name}.html")

    def convert_q_data(self, data: pd.DataFrame, code_dict: dict, col_name):
        q_m = {"A": 0, "B": 1, "C": 2, "D": 3}
        q_dict_data = {}
        for index in data.index:
            ele = dict(data.loc[index])
            time = ele['time']
            i = q_m.get(time[4])
            year = time[0:4]
            value = ele[col_name]
            code_name = code_dict.get(col_name)
            combine_name = f"{year}{code_name}"
            q_dict_data.setdefault(combine_name, [0, 0, 0, 0])
            q_dict_data[combine_name][i] = value
        df = pd.DataFrame(q_dict_data, index=['一季度', '二季度', '三季度', '四季度'])
        return df

    def generator_analysis_html(self):
        """
        0.GDP数据
        1.pmi数据 pmi 预测
        2.cpi数据
        3.社融数据
        4.海关数据
        5.财政数据
        6.消费数据，
        7.房地产数据
        8.工业数据分析
        :return:
        """
        chart_list = []

        change_gdp_code_dict = {'A010101_jd': '国内生产总值_当季值', 'A010102_jd': '国内生产总值_累计值',
                                'A010103_jd': '第一产业增加值_当季值', 'A010104_jd': '第一产业增加值_累计值',
                                'A010105_jd': '第二产业增加值_当季值', 'A010106_jd': '第二产业增加值_累计值',
                                'A010107_jd': '第三产业增加值_当季值', 'A010108_jd': '第三产业增加值_累计值',
                                'A010109_jd': '农林牧渔业增加值_当季值', 'A01010A_jd': '农林牧渔业增加值_累计值',
                                'A01010B_jd': '工业增加值_当季值', 'A01010C_jd': '工业增加值_累计值',
                                'A01010D_jd': '制造业增加值_当季值', 'A01010E_jd': '制造业增加值_累计值',
                                'A01011D_jd': '建筑业增加值_当季值', 'A01011E_jd': '建筑业增加值_累计值',
                                'A01011F_jd': '批发和零售业增加值_当季值', 'A01011G_jd': '批发和零售业增加值_累计值',
                                'A01011H_jd': '交通运输、仓储和邮政业增加值_当季值',
                                'A01011I_jd': '交通运输、仓储和邮政业增加值_累计值',
                                'A01011J_jd': '住宿和餐饮业增加值_当季值',
                                'A01011K_jd': '住宿和餐饮业增加值_累计值', 'A01011L_jd': '金融业增加值_当季值',
                                'A01011M_jd': '金融业增加值_累计值', 'A01011N_jd': '房地产业增加值_当季值',
                                'A01011O_jd': '房地产业增加值_累计值',
                                'A01011P_jd': '信息传输、软件和信息技术服务业增加值_当季值',
                                'A01011Q_jd': '信息传输、软件和信息技术服务业增加值_累计值',
                                'A01011R_jd': '租赁和商务服务业增加值_当季值',
                                'A01011S_jd': '租赁和商务服务业增加值_累计值',
                                'A01012P_jd': '其他行业增加值_当季值', 'A01012Q_jd': '其他行业增加值_累计值'}
        self.gdp_bar_line_tab_data(change_gdp_code_dict, file_name="变价gdp分析")

        no_change_gdp_dict = {'A010201_jd': '国内生产总值(不变价)_当季值', 'A010202_jd': '国内生产总值(不变价)_累计值',
                              'A010203_jd': '第一产业增加值(不变价)_当季值',
                              'A010204_jd': '第一产业增加值(不变价)_累计值',
                              'A010205_jd': '第二产业增加值(不变价)_当季值',
                              'A010206_jd': '第二产业增加值(不变价)_累计值',
                              'A010207_jd': '第三产业增加值(不变价)_当季值',
                              'A010208_jd': '第三产业增加值(不变价)_累计值'}
        self.gdp_bar_line_tab_data(no_change_gdp_dict, file_name="不变价gdp分析")

        supply_gdb_dict = {'A010501_jd': '最终消费支出对国内生产总值增长贡献率_当季值',
                           'A010502_jd': '最终消费支出对国内生产总值增长贡献率_累计值',
                           'A010503_jd': '资本形成总额对国内生产总值增长贡献率_当季值',
                           'A010504_jd': '资本形成总额对国内生产总值增长贡献率_累计值',
                           'A010505_jd': '货物和服务净出口对国内生产总值增长贡献率_当季值',
                           'A010506_jd': '货物和服务净出口对国内生产总值增长贡献率_累计值'}
        data = self.get_data_from_cn_st(supply_gdb_dict, time='201001')
        tab = Tab()
        for code, name in supply_gdb_dict.items():
            df = self.convert_q_data(data, supply_gdb_dict, code)
            temp_chart = []
            self.df_to_chart(df, temp_chart)
            tab.add(temp_chart[0], name)

        three_supply_gdb_dict = {'A010601_jd': '国内生产总值贡献率_当季值', 'A010602_jd': '国内生产总值贡献率_累计值',
                                 'A010603_jd': '第一产业贡献率_当季值', 'A010604_jd': '第一产业贡献率_累计值',
                                 'A010605_jd': '第二产业贡献率_当季值', 'A010606_jd': '第二产业贡献率_累计值',
                                 'A010607_jd': '第三产业贡献率_当季值', 'A010608_jd': '第三产业贡献率_累计值'}
        data = self.get_data_from_cn_st(three_supply_gdb_dict, time='201001')
        for code, name in three_supply_gdb_dict.items():
            df = self.convert_q_data(data, three_supply_gdb_dict, code)
            temp_chart = []
            self.df_to_chart(df, temp_chart)
            tab.add(temp_chart[0], name)
        tab.render(f"{self.analysis_dir}/supply_ddp.html")

        # PMI数据可视化
        pmi_code_dict = {'A0B0101_yd': '制造业采购经理指数(%)',
                         'A0B0102_yd': '生产指数(%)',
                         'A0B0103_yd': '新订单指数(%)',
                         'A0B0104_yd': '新出口订单指数(%)',
                         'A0B0301_yd': '综合PMI产出指数(%)',
                         'A0B010C_yd': '从业人员指数',
                         'A0B010D_yd': '供应商配送时间指数',
                         'A0B010B_yd': '原材料库存指数',
                         }
        data = self.get_data_from_cn_st(pmi_code_dict, time='201001')
        data.rename(columns=pmi_code_dict, inplace=True)
        self.df_to_chart(data, chart_list, cols=list(pmi_code_dict.values()), index_col_key='time', chart_type='line')
        # CPI数据可视化
        cpi_code_dict = {
            "A01010101_yd": "居民消费价格指数(上年同月=100)",
            "A01020101_yd": "居民消费价格指数(上年同期=100)",
            "A01030101_yd": "居民消费价格指数(上月=100)",
        }
        data = self.get_data_from_cn_st(cpi_code_dict, time='201001')
        data.rename(columns=cpi_code_dict, inplace=True)
        self.df_to_chart(data, chart_list, cols=list(cpi_code_dict.values()), index_col_key='time', chart_type='line')

        # 社融数据
        agg_stock_dict = {
            "社融规模增量数据": "afre",
            "人民币贷款": "rmb_loans",
            "委托贷款": "entrusted_loans",
            "信托贷款": "trust_loans",
            "未贴现银行承兑汇票": "undiscounted_banker_acceptances",
            "企业债券": "net_fin_cor_bonds",
            "政府债券": "gov_bonds",
        }
        agg_stock_dict = {v: k for k, v in agg_stock_dict.items()}
        # 1.增量分析
        data = self.get_data_from_seq_data(data_type='credit_funds', metric_code_list=['agg_fin_flow'], time='2010',
                                           val_keys=list(agg_stock_dict.keys()))
        """
        2.分类别看 
            2.1 表内信贷:人民币贷款,外币贷款
            2.2 表外融资：未贴现银行承兑汇票,信托贷款,委托贷款
            2.3 直接融资：非金融企业境内股票融资,企业债券
            2.4 其他 公司赔偿、投资性房地产、小额货款合同、贷款公司贷款、存款性金融机构资产支持证券(2018年7月纳人)、贷款核销(2018年7月纳人)、政府债券(含地方专项债券、地方一般债券、国债)。
        """
        for k, v in agg_stock_dict.items():
            ele_data = convert_pd_data_to_month_data(data, 'time', k, 'metric_code', {"agg_fin_flow": v})
            self.df_to_chart(ele_data, chart_list, chart_type='line')
        # 企业和住户短期和长期贷款分析
        income_config = {
            "住户贷款": "loans_to_households",
            "住户短期贷款": "short_term_loans",
            "住户中长期贷款": "mid_long_term_loans",

            "(事)业单位贷款": "loans_to_non_financial_enterprises_and_government_departments_organizations",
            "企业短期贷款": "short_term_loans_1",
            "企业中长期贷款": "mid_long_term_loans_1",
            "债券投资": "portfolio_investments",
            "票据融资": "paper_financing"
        }
        income_config = {v: k for k, v in income_config.items()}
        data = self.get_data_from_seq_data(data_type='credit_funds', metric_code_list=['credit_funds_fin_inst_rmb'],
                                           time='2010', val_keys=list(income_config.keys()))
        for k, v in income_config.items():
            ele_data = data[[k, 'time', 'metric_code']]
            ele_data[k] = ele_data[k].diff()
            ele_data = convert_pd_data_to_month_data(ele_data, 'time', k, 'metric_code',
                                                     {"credit_funds_fin_inst_rmb": v})
            self.df_to_chart(ele_data, chart_list, chart_type='line')

        # m1和m2剪刀差
        money_code_dict = {"A0D0102_yd": "货币和准货币(M2)供应量同比增长(%)", "A0D0104_yd": "货币(M1)供应量同比增长(%)",
                           "A0D0106_yd": "流通中现金(M0)供应量同比增长(%)"}
        data = self.get_data_from_cn_st(money_code_dict, time='201001')
        data['m1_m2_diff'] = round(data['A0D0104_yd'] - data['A0D0102_yd'], 4)
        data['code'] = 'm1与m2增速之差'
        m1_m2_diff = convert_pd_data_to_month_data(data, 'time', 'm1_m2_diff', 'code')
        self.df_to_chart(m1_m2_diff, chart_list, chart_type='line')

        # 海关进出口数据
        condition = {"data_type": "country_export_import", "name": "总值", "date": {"$gte": '2018'}}
        dict_key_mapping = {
            "acc_export_amount_cyc": "出口金额累计同比",
            "acc_import_amount_cyc": "进口金额累计同比",
            "acc_export_import_amount_cyc": "进出口金额累计同比",
            "acc_export_amount": "出口金额累计(亿元)",
        }
        data = self.get_data_from_board(condition=condition, is_cal=False, val_keys=list(dict_key_mapping.keys()))
        data['acc_export_amount'] = round(data['acc_export_amount'] / 1e4, 4)
        data['date'] = data['date'].apply(lambda ele: ele.replace("-", ""))
        tab = Tab()
        for k, v in dict_key_mapping.items():
            temp_chart_list = []
            new_data = convert_pd_data_to_month_data(data, 'date', k, 'name', {"总值": v})
            self.df_to_chart(new_data, temp_chart_list, chart_type='line')
            tab.add(temp_chart_list[0], v)
        # tab.render("test.html")
        # chart_list.append(tab)
        # 财政数据
        all_income_config = {
            "全国一般公共预算收入": "all_public_budget_revenue",
            "中央一般公共预算收入": "center_public_budget_revenue",
            "地方一般公共预算本级收入": "region_public_budget_revenue",
            "税收收入": "all_tax_revenue",
            "非税收入": "non_tax_revenue",
            # 企业相关
            "国内增值税": "tax_on_added_val",
            "企业所得税": "business_income_tax",
            # 个人相关
            "个人所得税": "personal_income_tax",
            # 出口
            "出口退税": "export_board_tax",
            # 房地产相关
            "契税": "deed_tax",
            "房产税": "building_tax",
            "土地增值税": "land_val_incr_tax",
            "耕地占用税": "occ_farm_land_tax",
            "城镇土地使用税": "town_land_use_tax",
        }
        data = self.get_data_from_seq_data(data_type='gov_fin', metric_code_list=['gov_fin_data'], time='2010',
                                           val_keys=list(all_income_config.values()))
        tab = Tab()
        # show_data(data)
        for v, k in all_income_config.items():
            temp_chart_list = []
            data[k] = round(data[k] / 1e4, 4)
            new_data = convert_pd_data_to_month_data(data, 'time', k, 'metric_code', {"gov_fin_data": v})
            # b_cols = list(new_data.columns)
            # for col in b_cols:
            #     new_data[col+"1"] = new_data[col].shift(1)
            #     new_data.fillna(0,inplace=True)
            #     new_data[col] = new_data[col]-new_data[col+"1"]
            # new_data = new_data[b_cols]
            new_data = new_data.diff()
            self.df_to_chart(new_data, temp_chart_list, chart_type='line')
            tab.add(temp_chart_list[0], v)
        # tab.render("test.html")
        # 工业产成平存货分析
        inventory_code_dict = {'A020A1S_yd': '营业收入_累计增长', 'A020A1Q_yd': '营业收入_累计值',
                               'A020A0E_yd': '产成品存货_本月末', 'A020A0G_yd': '产成品存货_增减',
                               'A020A0D_yd': '存货_增减', 'A020A0B_yd': '存货_本月末',
                               'A020A1G_yd': '利润总额_累计增长', 'A020A1E_yd': '利润总额_累计值',
                               'A020A0J_yd': '资产总计_增减', 'A020A0H_yd': '资产总计_本月末',
                               'A020A0K_yd': '负债合计_本月末', 'A020A0M_yd': '负债合计_增减',
                               }
        data = self.get_data_from_cn_st(inventory_code_dict, time='201001')
        data = self.tool_filter_month_data(data)
        data['资产负债率'] = round(data['A020A0K_yd']/data['A020A0H_yd'],4)
        tab = Tab()
        plot_inventory_list = [
            {"name": "存货分析", "line": ["A020A0D_yd", "存货同比(%)"], "bar": ["A020A0B_yd", "存货本月末(亿元)"]},
            {"name": "产成品存货分析", "line": ["A020A0G_yd", "产成品存货同比(%)"],
             "bar": ["A020A0E_yd", "产成品存货本月末(亿元)"]},
            {"name": "营业收入分析", "line": ["A020A1S_yd", "营业收入累计增长(%)"],
             "bar": ["A020A1Q_yd", "营业收入累计值(亿元)"]},
            {"name": "利润总额分析", "line": ["A020A1G_yd", "利润累计增长(%)"],
             "bar": ["A020A1E_yd", "利润总额累计值(亿元)"]},
            {"name": "资产分析", "line": ["A020A0J_yd", "资产增减(%)"],
             "bar": ["A020A0H_yd", "资产累计值(亿元)"]},
            {"name": "负债分析", "line": ["A020A0M_yd", "负债增减(%)"],
             "bar": ["A020A0K_yd", "负债累计值(亿元)"]},
            {"name": "资产负债率", "line": ["资产负债率", "资产负债率"],
             "bar": ["A020A0K_yd", "负债累计值(亿元)"]},
        ]
        for combine_dict in plot_inventory_list:
            line_code, line_name = combine_dict['line']
            bar_code, bar_name = combine_dict['bar']
            bar = self.bar_line_overlap(list(data['time'].values), {bar_name: list(data[bar_code].values)},
                                        {line_name: list(data[line_code].values)})

            tab.add(bar, combine_dict['name'])
        tab.render(f"{self.analysis_dir}/工业存货分析.html")

        # page = Page()
        # for char in chart_list:
        #     page.add(char)
        # tab.add(page,'page_test')
        # tab.render(f"{self.name}.html")
        # page.render(f"{self.name}.html")


if __name__ == '__main__':
    # enter_big_model_analysis_macro()
    # df = cn_pmi_analysis()
    # print(df)
    cn_macro = CNMacroAnalysis()
    cn_macro.generator_analysis_html()
    pass
