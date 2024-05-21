import pandas as pd

from data.mongodb import get_mongo_table
from data.stock_metric_data import stock_indicator, stock_vol_and_name
from utils.tool import sort_dict_data_by, dump_json_data, load_json_data
from utils.actions import show_data
from indicator.common_indicator import get_stock_last_dzjy, get_batch_stock_margin_indicator
import os
from datetime import datetime, timedelta
import akshare as ak


def get_stock_and_industry():
    """
    按行业获取股票
    :return:
    """
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False})
    industry_dict_data = {}
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        industry = ticker['industry']
        name = ticker['name']
        code, lr = ts_code.split(".")
        if lr in ['SZ', 'SH']:
            industry_dict_data.setdefault(industry, [])
            industry_dict_data[industry].append({"name": name, "fin_code": lr.lower() + code, "code": code})
    return industry_dict_data


def get_stock_metric_data(codes):
    """
    获取股票基础指标
    :param codes:
    :return:
    """
    stock_common = get_mongo_table(collection='stock_common')
    mv_mapping = {}
    tickers_cursor = stock_common.find(
        {"metric_code": {"$in": codes}, "data_type": 'stock_indicator'},
        projection={'_id': False})
    for cursor in tickers_cursor:
        code = str(cursor['metric_code'])
        total_mv = round(cursor['total_mv'] / 1e8, 4)
        cursor['total_mv'] = total_mv
        mv_mapping[code] = cursor
    return mv_mapping


def get_stock_fhps(dates=None):
    """
    股票分红的数据
    :param dates:
    :return:
    """
    if dates is None:
        dates = ['20221231', '20230630', '20231231']
    fh_st_dict = {}
    cyear = max([ele[0:4] for ele in dates])
    print(f"处理的年份是 {cyear}")
    print(f"处理的日期是 {dates}")
    file_name = f'fhstock{cyear}.json'

    if os.path.exists(file_name) is True:
        return load_json_data(file_name)
    for date in dates:
        stock_fhps_em_df = ak.stock_fhps_em(date=date)
        for index in stock_fhps_em_df.index:
            dict_data = dict(stock_fhps_em_df.loc[index])
            stock_rate = dict_data['现金分红-股息率']
            code = dict_data['代码']
            name = dict_data['名称']
            time = str(dict_data['除权除息日'])
            year = time[0:4]
            if year == cyear:
                combine_key = f"{code}_{name}"
                fh_st_dict.setdefault(combine_key, 0)
                if str(stock_rate) == 'nan':
                    stock_rate = 0
                fh_st_dict[combine_key] += stock_rate
    data = sort_dict_data_by(fh_st_dict, by='value')
    dump_json_data(file_name, data)
    return data


def get_stock_holder_or_reduce(codes):
    """
    获取股东减持还是增持
    :param codes:
    :return:
    """
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    before_day60 = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"before day {before_day60}")
    result_dict_data = {}
    for ele in stock_seq_daily.find({"metric_key": {"$in": codes}, "ann_time": {"$gte": before_day60}},
                                    projection={'_id': False}):
        code = ele['metric_key']
        holder_or_reduce = ele['shareholding_change_overweight']
        shareholding_change_outstanding_share_rate = ele['shareholding_change_outstanding_share_rate']
        if shareholding_change_outstanding_share_rate != '':
            shareholding_change_outstanding_share_rate = float(shareholding_change_outstanding_share_rate)
        else:
            shareholding_change_outstanding_share_rate = 0
        if holder_or_reduce == '减持':
            shareholding_change_outstanding_share_rate = -shareholding_change_outstanding_share_rate
        result_dict_data.setdefault(code, 0)
        result_dict_data[code] += shareholding_change_outstanding_share_rate
    risk_level = {}
    for k, v in result_dict_data.items():
        if v > 0:
            risk_level[k] = "无风险"
        if v < 0:
            risk_level[k] = "有风险"
    return risk_level


def get_stock_by_roe(codes, get_date=None):
    fin_col = get_mongo_table(collection='fin_simple')
    before_year_str = (datetime.now() - timedelta(days=365)).strftime("%Y0101")
    result_roe = {}
    ret = fin_col.find(
        {"code": {"$in": codes}, "date": {"$gt": before_year_str}, "data_type": {"$in": ['zcfz', 'lrb']}},
        projection={'_id': False, 'code': True, 'income': True, 'name': True, 'lia_assets': True, 'total_assets': True,
                    'date': True, 'data_type': True}).sort("date")
    for ele in ret:
        code = ele['code']
        date = ele['date']
        data_type = ele['data_type']
        result_roe.setdefault(date, {})
        if data_type == 'lrb':
            income = ele['income']
            if code not in result_roe[date].keys():
                result_roe[date][code] = {"income": income}
            else:
                result_roe[date][code]['income'] = income
        if data_type == 'zcfz':
            lia_assets = ele['lia_assets']
            total_assets = ele['total_assets']
            net_assets = total_assets - lia_assets
            if code not in result_roe[date].keys():
                result_roe[date][code] = {"net_assets": net_assets}
            else:
                result_roe[date][code]['net_assets'] = net_assets
    final_result = {}
    result_roe = sort_dict_data_by(result_roe, reverse=True)
    for time, combine in result_roe.items():
        if get_date is None and len(final_result.keys()) == 0:
            final_result[time] = {}
            for code, cv in combine.items():
                if 'income' in cv.keys() and 'net_assets' in cv.keys():
                    net_profit = round(cv['income'] / cv['net_assets'], 4)
                    final_result[time][code] = net_profit
        if get_date is not None and get_date == time:
            final_result[time] = {}
            for code, cv in combine.items():
                if 'income' in cv.keys() and 'net_assets' in cv.keys():
                    net_profit = round(cv['income'] / cv['net_assets'], 4)
                    final_result[time][code] = net_profit
    return final_result.values()


def handle_score(row, col_list):
    total_score = 0
    for col in col_list:
        total_score += row[col]
    total_score = round(total_score, 4)
    return total_score


def metric_rank_score(dict_data, metric, sort_type=False):
    dict_list = []
    sort_dict_data = sort_dict_data_by(dict_data, by='value', reverse=sort_type)
    num = len(sort_dict_data)
    before_rank, before_ele = 0, 0
    for i, combine in enumerate(sort_dict_data.items()):
        k, v = combine
        if i == 0:
            before_rank, before_ele = i + 1, v
        else:
            if before_ele != v:
                before_rank += 1
            before_ele = v
        score = round((before_rank / num) * 100, 4)
        dict_list.append({"code": k, f"{metric}_score": score})
    return dict_list


def get_common_concept_stock_pool(concept_names):
    now_str = datetime.now().strftime("%Y%m%d")
    before_7_day_str = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    concept_code_pd = {}
    for concept_name in concept_names:
        file_name = f"{now_str}_{concept_name}_data.csv"
        if os.path.exists(file_name):
            code_pd = pd.read_csv(file_name, dtype=str)
        else:
            stock_board_concept_cons_ths_df = ak.stock_board_concept_cons_ths(symbol=concept_name)
            datas = []
            for index in stock_board_concept_cons_ths_df.index:
                code = stock_board_concept_cons_ths_df.loc[index]['代码']
                code_name = stock_board_concept_cons_ths_df.loc[index]['名称']
                pe = stock_board_concept_cons_ths_df.loc[index]['市盈率']
                flow_mv = stock_board_concept_cons_ths_df.loc[index]['流通市值']
                turn_over = stock_board_concept_cons_ths_df.loc[index]['换手']
                data = {"code": code, "code_name": code_name, "pe": pe, "flow_mv": flow_mv, "turn_over": turn_over}
                datas.append(data)
            code_pd = pd.DataFrame(datas)
            code_pd.to_csv(file_name, index=False)
        concept_code_pd[concept_name] = code_pd

    code_mapping = {}
    metric_core_col = ['pe_score', 'turn_over_score', 'flow_mv_score', 'roe_score']
    score_dict = {"pe": True, "turn_over": True}
    for k, v in concept_code_pd.items():
        print(f"handle name={k}")
        codes = []
        # 按 pe 排名
        pe_dict_data = {}
        # 按 flow_mv 排名
        flow_mv_dict = {}
        # 按 turn_over 排名
        turn_over_dict = {}
        for index in v.index:
            dict_data = dict(v.loc[index])
            code = dict_data['code']
            codes.append(code)
            code_name = dict_data['code_name']
            code_mapping[code] = code_name
            pe = dict_data['pe']
            flow_mv = dict_data['flow_mv']
            turn_over = dict_data['turn_over']
            if pe != '--' and float(pe) < 0:
                pe = 100000000000 * 2
            if pe == '--':
                pe = 100000000000
            else:
                pe = float(pe)
            if turn_over == '--':
                turn_over = 100000000000
            pe_dict_data[code] = pe
            turn_over_dict[code] = float(turn_over)
            flow_mv_dict[code] = float(flow_mv.replace("亿", ""))

        res = metric_rank_score(pe_dict_data, 'pe', sort_type=score_dict.get('pe', False))
        score_df = pd.DataFrame(res)

        res = metric_rank_score(turn_over_dict, 'turn_over', sort_type=score_dict.get('turn_over', False))
        turn_over_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, turn_over_score_df, on=['code'], how='left')

        res = metric_rank_score(flow_mv_dict, 'flow_mv', sort_type=score_dict.get('flow_mv', False))
        flow_mv_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, flow_mv_score_df, on=['code'], how='left')

        roe = list(get_stock_by_roe(codes, get_date='20230930'))[0]
        res = metric_rank_score(roe, 'roe', sort_type=score_dict.get('roe', False))
        roe_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, roe_score_df, on=['code'], how='left')

        holder_or_reduce = get_stock_holder_or_reduce(codes)

        dzjy_dict = get_stock_last_dzjy(codes, before_7_day_str)
        dzjy_dict = {code: ele['risk_level'] for code, ele in dzjy_dict.items()}

        margin_risk_dict = get_batch_stock_margin_indicator(codes)
        convert_margin_risk_dict = {}
        for code, combine_risk in margin_risk_dict.items():
            convert_margin_str = ''
            if '低风险' in combine_risk.keys():
                convert_margin_str += "低风险=" + str(combine_risk.get('低风险'))
            if '有风险' in combine_risk.keys():
                convert_margin_str += "有风险=" + str(combine_risk.get('有风险'))
            convert_margin_risk_dict[code] = convert_margin_str

        score_df.fillna(0, inplace=True)
        score_df['total_score'] = score_df.apply(handle_score, axis=1, args=(metric_core_col,))
        score_df.sort_values(by='total_score', ascending=False, inplace=True)
        new_data_list = []
        for index in score_df.index:
            dict_data = dict(score_df.loc[index])
            dict_data['name'] = code_mapping.get(dict_data.get("code"))
            dict_data['30day_reduce_risk'] = holder_or_reduce.get(dict_data.get("code"), '无风险')
            dict_data['7day_dajy_risk'] = dzjy_dict.get(dict_data.get("code"), '无风险')
            dict_data['margin_risk'] = convert_margin_risk_dict.get(dict_data.get("code"), '无风险')
            new_data_list.append(dict_data)
        pd.DataFrame(new_data_list).to_csv(f"{now_str}_{k}_stock.csv", index=False)
        print("*" * 50)


def get_news_top2_concept_stock_pool():
    """
    最新的概念股票池
    :return:
    """
    now_str = datetime.now().strftime("%Y%m%d")
    concept_path = f"{now_str}_concept.csv"
    before_7_day_str = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    if os.path.exists(concept_path):
        concept_names = pd.read_csv(concept_path)
    else:
        concept_names = ak.stock_board_concept_name_ths()
        concept_names.to_csv(concept_path, index=False)
    concept_names.sort_values(by='日期', inplace=True, ascending=False)
    show_data(concept_names.head(10))
    concept_names_top2 = concept_names.head(2)['概念名称'].values
    concept_code_pd = {}
    for concept_name in concept_names_top2:
        file_name = f"concept_new_top2_{now_str}_{concept_name}_data.csv"
        if os.path.exists(file_name):
            code_pd = pd.read_csv(file_name, dtype=str)
        else:
            stock_board_concept_cons_ths_df = ak.stock_board_concept_cons_ths(symbol=concept_name)
            datas = []
            for index in stock_board_concept_cons_ths_df.index:
                code = stock_board_concept_cons_ths_df.loc[index]['代码']
                code_name = stock_board_concept_cons_ths_df.loc[index]['名称']
                pe = stock_board_concept_cons_ths_df.loc[index]['市盈率']
                flow_mv = stock_board_concept_cons_ths_df.loc[index]['流通市值']
                turn_over = stock_board_concept_cons_ths_df.loc[index]['换手']
                data = {"code": code, "code_name": code_name, "pe": pe, "flow_mv": flow_mv, "turn_over": turn_over}
                datas.append(data)
            code_pd = pd.DataFrame(datas)
            code_pd.to_csv(file_name, index=False)
        concept_code_pd[concept_name] = code_pd

    code_mapping = {}
    metric_core_col = ['pe_score', 'turn_over_score', 'flow_mv_score', 'roe_score']
    score_dict = {"pe": True, "turn_over": True}
    for k, v in concept_code_pd.items():
        print(f"handle name={k}")
        codes = []
        # 按 pe 排名
        pe_dict_data = {}
        # 按 flow_mv 排名
        flow_mv_dict = {}
        # 按 turn_over 排名
        turn_over_dict = {}
        for index in v.index:
            dict_data = dict(v.loc[index])
            code = dict_data['code']
            codes.append(code)
            code_name = dict_data['code_name']
            code_mapping[code] = code_name
            pe = dict_data['pe']
            flow_mv = dict_data['flow_mv']
            turn_over = dict_data['turn_over']
            if pe!='--' and float(pe)<0:
                pe = 100000000000*2
            if pe == '--':
                pe = 100000000000
            else:
                pe = float(pe)
            pe_dict_data[code] = pe
            if turn_over == '--':
                turn_over = 100000000000
            turn_over_dict[code] = float(turn_over)
            flow_mv_dict[code] = float(flow_mv.replace("亿", ""))

        res = metric_rank_score(pe_dict_data, 'pe', sort_type=score_dict.get('pe', False))
        score_df = pd.DataFrame(res)

        res = metric_rank_score(turn_over_dict, 'turn_over', sort_type=score_dict.get('turn_over', False))
        turn_over_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, turn_over_score_df, on=['code'], how='left')

        res = metric_rank_score(flow_mv_dict, 'flow_mv', sort_type=score_dict.get('flow_mv', False))
        flow_mv_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, flow_mv_score_df, on=['code'], how='left')

        roe = list(get_stock_by_roe(codes))[0]
        res = metric_rank_score(roe, 'roe', sort_type=score_dict.get('roe', False))
        roe_score_df = pd.DataFrame(res)
        score_df = pd.merge(score_df, roe_score_df, on=['code'], how='left')

        holder_or_reduce = get_stock_holder_or_reduce(codes)

        dzjy_dict = get_stock_last_dzjy(codes, before_7_day_str)
        dzjy_dict = {code: ele['risk_level'] for code, ele in dzjy_dict.items()}

        margin_risk_dict = get_batch_stock_margin_indicator(codes)
        convert_margin_risk_dict = {}
        for code, combine_risk in margin_risk_dict.items():
            convert_margin_str = ''
            if '低风险' in combine_risk.keys():
                convert_margin_str += "低风险=" + str(combine_risk.get('低风险'))
            if '有风险' in combine_risk.keys():
                convert_margin_str += "有风险=" + str(combine_risk.get('有风险'))
            convert_margin_risk_dict[code] = convert_margin_str

        score_df.fillna(0, inplace=True)
        score_df['total_score'] = score_df.apply(handle_score, axis=1, args=(metric_core_col,))
        score_df.sort_values(by='total_score', ascending=False, inplace=True)
        new_data_list = []
        for index in score_df.index:
            dict_data = dict(score_df.loc[index])
            dict_data['name'] = code_mapping.get(dict_data.get("code"))
            dict_data['30day_reduce_risk'] = holder_or_reduce.get(dict_data.get("code"), '无风险')
            dict_data['7day_dajy_risk'] = dzjy_dict.get(dict_data.get("code"), '无风险')
            dict_data['margin_risk'] = convert_margin_risk_dict.get(dict_data.get("code"), '无风险')
            new_data_list.append(dict_data)
        pd.DataFrame(new_data_list).to_csv(f"concept_new_top2_{now_str}_{k}_stock.csv", index=False)
        print("*" * 50)


def find_industry_stock_data():
    # 行业归类获取股票
    industry_stock_dict = get_stock_and_industry()
    for industry, stock_infos in industry_stock_dict.items():
        if industry != '软件服务':
            codes = [ele['code'] for ele in stock_infos]
            # 股票基础指标

            stock_indicator(codes)
            stock_vol_and_name(codes)
        print(industry, stock_infos)
    # 更加行业，选择股票，按市值,pe，或者财报打分等等指标，粗滤
    soft_services = industry_stock_dict['软件服务']

    codes = [ele['code'] for ele in soft_services]
    fin_codes = [ele['fin_code'] for ele in soft_services]
    code_name_mapping = {ele['code']: ele['name'] for ele in soft_services}
    metric_data = get_stock_metric_data(codes)
    print("市值排名")
    top100_total_mv_dict = sort_dict_data_by(
        {code: ele['total_mv'] for code, ele in metric_data.items() if str(ele['total_mv']) != 'nan'}, by='value',
        reverse=True)
    start = 0
    for code, mv in top100_total_mv_dict.items():
        name = code_name_mapping.get(code)
        print(name, code, mv)
        start += 1
        if start >= 100:
            break
    # 股票分红的数据
    stock_fh_dict = get_stock_fhps(dates=['20221231', '20230630', '20231231'])
    fh_dict_data = {}
    for combine_key, grate in stock_fh_dict.items():
        fh_code, name = combine_key.split("_")
        if fh_code in codes:
            fh_dict_data[fh_code] = grate
    top100_grate = sort_dict_data_by(fh_dict_data, by='value', reverse=True)
    start = 0
    print("分红排名")
    for code, grate in top100_grate.items():
        name = code_name_mapping.get(code)
        print(name, code, grate)
        start += 1
        if start >= 100:
            break
    gdd_hold_or_reduce = get_stock_holder_or_reduce(codes)
    print(gdd_hold_or_reduce)

    print("pe排名")
    top100_pe_dict = sort_dict_data_by(
        {code: ele['pe'] for code, ele in metric_data.items() if str(ele['pe']) != 'nan' and float(ele['pe'] > 0)},
        by='value',
        reverse=False)
    start = 0
    for code, mv in top100_pe_dict.items():
        name = code_name_mapping.get(code)
        print(name, code, mv)
        start += 1
        if start >= 100:
            break


if __name__ == '__main__':
    get_common_concept_stock_pool(['绿色电力'])
    get_common_concept_stock_pool(['电力物联网'])
    get_news_top2_concept_stock_pool()
