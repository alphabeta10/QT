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


if __name__ == '__main__':
    # enter_big_model_analysis_macro()
    df = cn_pmi_analysis()
    print(df)
