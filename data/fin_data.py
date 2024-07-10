import pandas as pd
import os
import re

import requests
from bs4 import BeautifulSoup
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def extract_data(name, raw_line):
    data, same_data = None, None
    result = re.findall(name + "(\d+\.?\d?)亿元，同比增长(\d+\.?\d?)%", raw_line)
    if len(result) > 0:
        data, same_data = result[0]
        same_data = float(same_data)
    else:
        result = re.findall(name + "(\d+\.?\d?)亿元，与上年同期持平", raw_line)
        if len(result) > 0:
            data = result[0]
            same_data = 0.0
        result = re.findall(name + "(\d+\.?\d?)亿元，同比下降(\d+\.?\d?)%", raw_line)
        if len(result) > 0:
            data, same_data = result[0]
            same_data = float(same_data) * -1
    if data is None:
        result = re.findall(name + "(\d+\.?\d?)亿元", raw_line)
        if len(result) > 0:
            data = result[0]
            same_data = 0.0
    if data is not None:
        return data,same_data
    return None


def gks_fin_data(filename="fin_txt20230601.txt"):
    day = filename[-12:-4]
    data_dict = {"time": day}

    all_income_config = {
        "全国一般公共预算收入": "all_public_budget_revenue",
        "中央一般公共预算收入": "center_public_budget_revenue",
        "地方一般公共预算本级收入": "region_public_budget_revenue",
        "税收收入": "all_tax_revenue",
        "非税收入": "non_tax_revenue",
    }

    all_expenditure_config = {
        "全国一般公共预算支出": "all_public_budget_expenditure",
        "中央一般公共预算本级支出": "center_public_budget_expenditure",
        "地方一般公共预算支出": "region_public_budget_expenditure",
    }

    detail_expenditure_config = {
        "教育支出": "education_expenditure",
        "科学技术支出": "science_and_tech_expenditure",
        "文化旅游体育与传媒支出": "cultural_tourism_expenditure",
        "社会保障和就业支出": "social_security_employment_expenditure",
        "卫生健康支出": "health_expenditure",
        "节能环保支出": "energy_env_protect_expenditure",
        "城乡社区支出": "urban_rural_expenditure",
        "农林水支出": "agr_wat_expenditure",
        "交通运输支出": "transportation_expenditure",
        "债务付息支出": "debt_interest_payments",
    }

    tax_config = {"国内增值税": "tax_on_added_val",
                  "国内消费税": "consum_tax_val",
                  "企业所得税": "business_income_tax",
                  "个人所得税": "personal_income_tax",
                  "进口货物增值税、消费税": "board_add_tax",
                  "。关税": "board_tax",
                  "出口退税": "export_board_tax",
                  "城市维护建设税": "city_maintain_tax",
                  "车辆购置税": "car_consum_tax",
                  "印花税": "stamp_tax",
                  "证券交易印花税": "securities_stamp_tax",
                  "资源税": "resource_tax",
                  "土地和房地产相关税收中，契税": "deed_tax",
                  "房产税": "building_tax",
                  "土地增值税": "land_val_incr_tax",
                  "耕地占用税": "occ_farm_land_tax",
                  "城镇土地使用税": "town_land_use_tax",
                  "环境保护税": "env_protect_tax",
                  "车船税、船舶吨税、烟叶税等其他各项税收收入合计": "other_tax"
                  }
    gov_fund_income_config = {
        "全国政府性基金预算收入": "gov_fund_budget_revenue",
        "中央政府性基金预算收入": "center_gov_fund_budget_revenue",
        "地方政府性基金预算本级收入": "region_gov_fund_budget_revenue",
        "国有土地使用权出让收入": "state_owned_land_revenue",
    }

    gov_expenditure_config = {
        "全国政府性基金预算支出": "gov_fund_budget_expenditure",
        "中央政府性基金预算本级支出": "center_gov_fund_budget_expenditure",
        "地方政府性基金预算支出": "region_gov_fund_budget_expenditure",
        "地方政府性基金预算相关支出": "region_gov_fund_budget_expenditure",
        "国有土地使用权出让收入相关支出": "state_owned_land_expenditure",
    }

    all_values = list(gov_expenditure_config.values()) + list(gov_fund_income_config.values()) + list(
        tax_config.values()) + list(detail_expenditure_config.values()) + list(all_expenditure_config.values()) + list(
        all_income_config.values())
    with open(filename, mode='r') as f:
        lines = f.readlines()
        for line in lines:
            # if '全国一般公共预算收入' in line:
            for key, val in all_income_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")
            # if "全国一般公共预算支出" in line:
            for key, val in all_expenditure_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")
            # if "全国政府性基金预算收入" in line:
            for key, val in gov_fund_income_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")
            # if "全国政府性基金预算支出" in line:
            for key, val in gov_expenditure_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")

            for key, val in tax_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")

            for key, val in detail_expenditure_config.items():
                if key in line:
                    result = extract_data(key, line)
                    if result is not None:
                        raw_data, raw_data_same = result
                        print(key + "=" + raw_data + "=" + str(raw_data_same))
                        data_dict[val] = raw_data
                        val_same = f"{val}_same"
                        data_dict[val_same] = raw_data_same
                    else:
                        print(f"{key}没有数据")
    is_pass = True
    for key in all_values:
        if key not in data_dict.keys():
            print(key, 'no pass')
            is_pass = False
    if is_pass:
        data_dict['metric_code'] = 'gov_fin_data'
        data_dict['data_type'] = 'gov_fin'
        return data_dict
    else:
        return None


def get_gov_data():
    dir = 'gov_fin_data2023'
    file_list = os.listdir(dir)
    update_request = []
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for file_name in file_list:
        file_path = os.path.join(dir, file_name)
        print(f"start handle {file_path}")
        dict_data = gks_fin_data(file_path)
        print(f"end handle {file_path}")
        if dict_data is not None:
            update_request.append(
                UpdateOne(
                    {"data_type": dict_data['data_type'], "time": dict_data['time'],
                     "metric_code": dict_data['metric_code']},
                    {"$set": dict_data},
                    upsert=True))
    if len(update_request) > 0:
        mongo_bulk_write_data(stock_common, update_request)


def find_data():
    all_income_config = {
        "all_public_budget_revenue_same": True,
        "center_public_budget_revenue_same": True,
        "region_public_budget_revenue_same": True,
        "all_tax_revenue_same": True,
        "non_tax_revenue_same": True,
        "time": True,
        "_id": False
    }

    income_config = {
        "全国一般公共预算收入": "all_public_budget_revenue_same",
        "中央一般公共预算收入": "center_public_budget_revenue_same",
        "地方一般公共预算本级收入": "region_public_budget_revenue_same",
        "税收收入": "all_tax_revenue_same",
        "非税收入": "non_tax_revenue_same",
    }

    income_config = {
        "住户贷款": "loans_to_households",
        "住户短期贷款": "short_term_loans",
        "住户中长期贷款": "mid_long_term_loans",
        "(事)业单位贷款": "loans_to_non_financial_enterprises_and_government_departments_organizations",
        "企业短期贷款": "short_term_loans_1",
        "企业中长期贷款": "mid_long_term_loans_1",
    }

    all_income_config = {
        "loans_to_households": True,
        "short_term_loans": True,
        "mid_long_term_loans": True,
        "time": True,
        "_id": False
    }

    all_income_config = {
        "afre_growth": True,
        "rmb_loans_growth": True,
        "trust_loans_growth": True,
        "time": True,
        "_id": False
    }

    income_config = {
        "社会融资贷款存量": "afre_growth",
        "人民币贷款存量": "rmb_loans_growth",
        "信贷存量": "trust_loans_growth",
    }

    re_all_config = {}
    for k, v in income_config.items():
        re_all_config[v] = k
    print(re_all_config)
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "credit_funds", "metric_code": "agg_fin_stock"},
                         projection=all_income_config).sort(
        "time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data.rename(columns=re_all_config)
    show_data(data)
    for col in income_config.keys():
        data[[col]] = data[[col]].astype(float)

    data.set_index(keys=['time'], inplace=True)
    show_data(data)
    # data['gk_teu_traffic_ptc'] = data['gk_teu_traffic'].pct_change(1)
    data.plot(kind='bar', title='航班', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def fin_ins_credit_funds_data(file_dir='fin_credit_funds2023', type_name='金融机构人民币信贷收支表', time='2023'):
    type_dict_mapping = {'金融机构外汇信贷收支表': 'fin_inst_foreign',
                         '金融机构本外币信贷收支表': 'fin_inst_rmb_foreign', '金融机构人民币信贷收支表': "fin_inst_rmb"}
    file_name = f"{file_dir}/{type_name}.xls"
    pd_data = pd.read_excel(file_name)
    mapping_dict = {'Unnamed: 1': f'{time}0101', 'Unnamed: 2': f'{time}0201',
                    'Unnamed: 3': f'{time}0301', 'Unnamed: 4': f'{time}0401', 'Unnamed: 5': f'{time}0501',
                    'Unnamed: 6': f'{time}0601',
                    'Unnamed: 7': f'{time}0701', 'Unnamed: 8': f'{time}0801', 'Unnamed: 9': f'{time}0901',
                    'Unnamed: 10': f'{time}1001',
                    'Unnamed: 11': f'{time}1101', 'Unnamed: 12': f'{time}1201'}
    convert_all_data_dict = {}
    type = type_dict_mapping[type_name]
    name_mapping_count = {}
    for index in pd_data.index:
        dict_data = dict(pd_data.loc[index])
        if str(dict_data['Unnamed: 1']) != 'nan' and str(dict_data['Unnamed: 1']) != f'{time}.01':
            item = dict_data[type_name]
            name = "_".join(re.findall('[A-Za-z]+', item)).lower()
            print(f"name={name}")
            if name not in name_mapping_count.keys():
                name_mapping_count[name] = 0
            name_mapping_count[name] += 1
            if name_mapping_count.get(name) > 1:
                name_count = name_mapping_count.get(name) - 1
                name = f"{name}_{name_count}"
            for col, new_col in mapping_dict.items():
                value = str(dict_data[col]).replace("\xa0", "")
                if new_col not in convert_all_data_dict.keys():
                    convert_all_data_dict[new_col] = {}
                convert_all_data_dict[new_col][name] = value
            if name == 'total_funds_uses':
                break
    update_request = []
    for k, v in convert_all_data_dict.items():
        new_dict_data = v
        new_dict_data['time'] = k
        new_dict_data['data_type'] = 'credit_funds'
        new_dict_data['metric_code'] = f'credit_funds_{type}'
        update_request.append(
            UpdateOne(
                {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                 "metric_code": new_dict_data['metric_code']},
                {"$set": new_dict_data},
                upsert=True)
        )
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')

    if update_request is not None:
        mongo_bulk_write_data(stock_common, update_request)


def go(file_dir='fin_agg2023', type_name='社会融资规模存量统计表', time='2023'):
    type_dict_mapping = {'社会融资规模增量统计表': 'agg_fin_flow',
                         '社会融资规模存量统计表': 'agg_fin_stock'}
    file_name = f"{file_dir}/{type_name}.xls"
    pd_data = pd.read_excel(file_name)

    agg_mapping_dict = {'Unnamed: 1': 'afre', 'Unnamed: 2': 'rmb_loans', 'Unnamed: 3': 'foreign_currency_loans',
                        'Unnamed: 4': 'entrusted_loans ', 'Unnamed: 5': 'trust_loans',
                        'Unnamed: 6': "undiscounted_banker_acceptances", 'Unnamed: 7': 'net_fin_cor_bonds',
                        'Unnamed: 8': 'gov_bonds',
                        'Unnamed: 9': 'equity_stock_non_fin_enter',
                        'Unnamed: 10': 'asset_backed_fin_inst',
                        'Unnamed: 11': 'loans_written_off'}
    update_request = []
    if type_name == '社会融资规模增量统计表':
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            month = str(dict_data['社会融资规模增量统计表'])
            if month[0:4] == time:
                convert_dict_data = {}
                convert_dict_data['time'] = month.replace(".", "") + "01"
                for col, name in agg_mapping_dict.items():
                    value = str(dict_data[col]).strip().replace("\xa0", "")
                    convert_dict_data[name] = value
                print(convert_dict_data)
                convert_dict_data['data_type'] = 'credit_funds'
                convert_dict_data['metric_code'] = type_dict_mapping[type_name]
                print(convert_dict_data)
                update_request.append(
                    UpdateOne(
                        {"data_type": convert_dict_data['data_type'], "time": convert_dict_data['time'],
                         "metric_code": convert_dict_data['metric_code']},
                        {"$set": convert_dict_data},
                        upsert=True)
                )
    if type_name == '社会融资规模存量统计表':
        data_dict_data = {}
        agg_stock_dict = {
            "人民币贷款": "rmb_loans",
            "外币贷款（折合人民币": "foreign_currency_loans",
            "委托贷款": "entrusted_loans",
            "信托贷款": "trust_loans",
            "未贴现银行承兑汇票": "undiscounted_banker_acceptances",
            "企业债券": "net_fin_cor_bonds",
            "政府债券": "gov_bonds",
            "非金融企业境内股票": "equity_stock_non_fin_enter",
            "存款类金融机构资产支持证券": "asset_backed_fin_inst",
            "贷款核销": "loans_written_off",
        }
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            name = str(dict_data['社会融资规模存量统计表']).strip().replace(" ", "")
            if name in agg_stock_dict.keys():
                col_name = agg_stock_dict[name]
                for i in range(1, 13):
                    first_index = i * 2 - 1
                    value = str(dict_data[f'Unnamed: {first_index}'])
                    second_index = i * 2
                    stock_percent = str(dict_data[f'Unnamed: {second_index}'])
                    if i < 10:
                        month = f"{time}0{i}01"
                    else:
                        month = f"{time}{i}01"
                    if month not in data_dict_data.keys():
                        data_dict_data[month] = {}
                    data_dict_data[month][f"{col_name}_value"] = value
                    data_dict_data[month][f"{col_name}_growth"] = stock_percent
        update_request = []
        for k, v in data_dict_data.items():
            new_dict_data = v
            new_dict_data['time'] = k
            new_dict_data['data_type'] = 'credit_funds'
            new_dict_data['metric_code'] = type_dict_mapping[type_name]
            update_request.append(
                UpdateOne(
                    {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                     "metric_code": new_dict_data['metric_code']},
                    {"$set": new_dict_data},
                    upsert=True)
            )

        # name = "_".join(re.findall('[A-Za-z]+',item)).lower()
        # if name not in name_mapping_count.keys():
        #     name_mapping_count[name] = 0
        # name_mapping_count[name] += 1
        # name_mapping[item] = name
        # for col,new_col in mapping_dict.items():
        #     value = str(dict_data[col]).replace("\xa0","")
        #     if new_col not in convert_all_data_dict.keys():
        #         convert_all_data_dict[new_col] = {}
        #     convert_all_data_dict[new_col][name] = value
        # if name=='total_funds_uses':
        #     break


def handle_credit_agg_stock_data(file_dir='fin_agg2023', type_name='社会融资规模存量统计表', time='2023'):
    type_dict_mapping = {'社会融资规模增量统计表': 'agg_fin_flow',
                         '社会融资规模存量统计表': 'agg_fin_stock'}
    file_name = f"{file_dir}/{type_name}.xls"
    pd_data = pd.read_excel(file_name)

    agg_mapping_dict = {'Unnamed: 1': 'afre', 'Unnamed: 2': 'rmb_loans', 'Unnamed: 3': 'foreign_currency_loans',
                        'Unnamed: 4': 'entrusted_loans', 'Unnamed: 5': 'trust_loans',
                        'Unnamed: 6': "undiscounted_banker_acceptances", 'Unnamed: 7': 'net_fin_cor_bonds',
                        'Unnamed: 8': 'gov_bonds',
                        'Unnamed: 9': 'equity_stock_non_fin_enter',
                        'Unnamed: 10': 'asset_backed_fin_inst',
                        'Unnamed: 11': 'loans_written_off'}
    update_request = []
    if type_name == '社会融资规模增量统计表':
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            month = str(dict_data['社会融资规模增量统计表'])
            if month[0:4] == time:
                convert_dict_data = {}
                convert_dict_data['time'] = month.replace(".", "") + "01"
                for col, name in agg_mapping_dict.items():
                    value = str(dict_data[col]).strip().replace("\xa0", "")
                    convert_dict_data[name] = value
                print(convert_dict_data)
                convert_dict_data['data_type'] = 'credit_funds'
                convert_dict_data['metric_code'] = type_dict_mapping[type_name]
                print(convert_dict_data)
                update_request.append(
                    UpdateOne(
                        {"data_type": convert_dict_data['data_type'], "time": convert_dict_data['time'],
                         "metric_code": convert_dict_data['metric_code']},
                        {"$set": convert_dict_data},
                        upsert=True)
                )
    if type_name == '社会融资规模存量统计表':
        data_dict_data = {}
        agg_stock_dict = {
            "社会融资规模存量": "afre",
            "人民币贷款": "rmb_loans",
            "外币贷款（折合人民币": "foreign_currency_loans",
            "委托贷款": "entrusted_loans",
            "信托贷款": "trust_loans",
            "未贴现银行承兑汇票": "undiscounted_banker_acceptances",
            "企业债券": "net_fin_cor_bonds",
            "政府债券": "gov_bonds",
            "非金融企业境内股票": "equity_stock_non_fin_enter",
            "存款类金融机构资产支持证券": "asset_backed_fin_inst",
            "贷款核销": "loans_written_off",
        }
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            name = str(dict_data['社会融资规模存量统计表']).strip().replace(" ", "")
            if name in agg_stock_dict.keys():
                col_name = agg_stock_dict[name]
                for i in range(1, 13):
                    first_index = i * 2 - 1
                    value = str(dict_data[f'Unnamed: {first_index}'])
                    second_index = i * 2
                    stock_percent = str(dict_data[f'Unnamed: {second_index}'])
                    if i < 10:
                        month = f"{time}0{i}01"
                    else:
                        month = f"{time}{i}01"
                    if month not in data_dict_data.keys():
                        data_dict_data[month] = {}
                    data_dict_data[month][f"{col_name}_value"] = value
                    data_dict_data[month][f"{col_name}_growth"] = stock_percent
        for k, v in data_dict_data.items():
            new_dict_data = v
            new_dict_data['time'] = k
            new_dict_data['data_type'] = 'credit_funds'
            new_dict_data['metric_code'] = type_dict_mapping[type_name]
            update_request.append(
                UpdateOne(
                    {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                     "metric_code": new_dict_data['metric_code']},
                    {"$set": new_dict_data},
                    upsert=True)
            )
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    if update_request is not None:
        mongo_bulk_write_data(stock_common, update_request)


def handle_balance_sheet_of_monetary_authority(handle_dir=None):
    if handle_dir is None:
        handle_dir = 'fin_balance_sheet_of_monetary_authority_his'
    list_files = os.listdir(handle_dir)
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for file_name in list_files:
        file_name = os.path.join(handle_dir, file_name)
        print(f"handle {file_name}")
        pd_data = pd.read_excel(file_name, dtype=str)
        meta_cols = None
        header_dict = {'国外资产ForeignAssets': 'foreign_assets', '外汇ForeignExchange': 'foreign_exchange',
                       '货币黄金MonetaryGold': 'monetary_gold',
                       '其他国外资产OtherForeignAssets': 'other_foreign_assets',
                       '对政府债权ClaimsonGovernment': 'claims_on_government',
                       '其中：中央政府Ofwhich:CentralGovernment': 'of_which_central_government',
                       '对其他存款性公司债权ClaimsonOtherDepositoryCorporations': 'claims_on_other_depository_corporations',
                       '对其他金融性公司债权ClaimsonOtherFinancialCorporations': 'claims_on_other_financial_corporations',
                       '对非金融性部门债权ClaimsonNon-financialSector': 'claims_on_non_financial_sector',
                       '其他资产OtherAssets': 'other_assets', '总资产TotalAssets': 'total_assets',
                       '储备货币ReserveMoney': 'reserve_money', '货币发行CurrencyIssue': 'currency_issue',
                       '金融性公司存款DepositsofFinancialCorporations': 'deposits_of_financial_corporations',
                       '其他存款性公司存款DepositsofOtherDepositoryCorporations': 'deposits_of_other_depository_corporations',
                       '其他金融性公司存款DepositsofOtherFinancialCorporations': 'deposits_of_other_financial_corporations',
                       '非金融机构存款DepositsofNon-financialInstitutions': 'deposits_of_non_financial_institutions',
                       '不计入储备货币的金融性公司存款DepositsoffinancialcorporationsexcludedfromReserveMoney': 'deposits_of_financial_corporations_excluded_from_reserve_money',
                       '发行债券BondIssue': 'bond_issue', '国外负债ForeignLiabilities': 'foreign_liabilities',
                       '政府存款DepositsofGovernment': 'deposits_of_government', '自有资金OwnCapital': 'own_capital',
                       '其他负债OtherLiabilities': 'other_liabilities', '总负债TotalLiabilities': 'total_liabilities'}
        result_data = {}
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])

            if str(dict_data['货币当局资产负债表']).replace(' ', '') == '项目Item':
                meta_cols = {key: str(ele).replace(".", "") + "01" for key, ele in dict_data.items() if
                             str(ele).replace(' ', '') != '项目Item' and len(str(ele)) == 7}
            if meta_cols is not None:
                if str(dict_data['货币当局资产负债表']).replace(' ', '') not in ['nan', '项目Item']:
                    item_name = str(dict_data['货币当局资产负债表']).replace(" ", "")
                    col_name = header_dict.get(item_name)
                    if col_name is not None:
                        for key, time_name in meta_cols.items():
                            value = str(dict_data.get(key)).replace("\u3000", "")
                            if str(value) == 'nan' or value == '':
                                value = 0
                            else:
                                value = float(value)
                            result_data.setdefault(time_name, {})
                            result_data[time_name][col_name] = value
                        if col_name == 'total_liabilities':
                            break
        update_request = []
        for time, new_dict_data in result_data.items():
            if len(new_dict_data) != len(header_dict):
                print("check data")
            new_dict_data['time'] = time
            new_dict_data['data_type'] = 'fin_monetary'
            new_dict_data['metric_code'] = 'balance_monetary_authority'
            update_request.append(
                UpdateOne(
                    {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                     "metric_code": new_dict_data['metric_code']},
                    {"$set": new_dict_data},
                    upsert=True))
        mongo_bulk_write_data(stock_common, update_request)


def enter_credit_fin():
    file_dir = 'fin_credit_funds2024'
    type_dict_mapping = {'金融机构外汇信贷收支表': 'fin_inst_foreign',
                         '金融机构本外币信贷收支表': 'fin_inst_rmb_foreign', '金融机构人民币信贷收支表': "fin_inst_rmb"}
    for key, v in type_dict_mapping.items():
        print(key)
        fin_ins_credit_funds_data(file_dir=file_dir, type_name=key, time='2024')


def enter_credit_fin_agg_flow():
    file_dir = 'fin_agg2024'
    type_dict_mapping = {'社会融资规模增量统计表': 'agg_fin_flow',
                         '社会融资规模存量统计表': 'agg_fin_stock'}
    for key, v in type_dict_mapping.items():
        handle_credit_agg_stock_data(file_dir=file_dir, type_name=key, time='2024')


def gov_revenue_expenditure(url):
    respond = requests.get(url)
    html = respond.content
    html_doc = str(html, 'utf-8')

    soup = BeautifulSoup(html_doc, 'html.parser')
    h2s = soup.find_all("h2", "title_con")
    print(h2s)
    if h2s is not None and len(h2s) > 0:
        h2 = h2s[0]
        header = h2.text
        year = int(header[0:4])
        result = re.findall("(\d+)年(\d+)月", header)
        month = None
        if len(result) > 0:
            month = int(result[0][1])
        result = re.findall("(\d+)年(\d+)-(\d+)月", header)
        if len(result) > 0:
            month = int(result[0][2])
        if '一季度' in header:
            month = '3'
        if '二季度' in header:
            month = '6'
        if '三季度' in header:
            month = '9'
        if '四季度' in header:
            month = '12'
        if '上半年' in header:
            month = '6'
        if '季度' not in header and '月' not in header and '上半年' not in header:
            month = '12'
        if month is not None:
            if int(month) < 10:
                month = int(month)
                month = f"0{month}"
            day = f"{year}{month}01"
            file_name = f'gov_fin_data2023/fin_txt{day}.txt'
            search_div = soup.find_all("div", 'TRS_Editor')
            if search_div is not None and len(search_div) > 0:

                print(search_div[0])
                ps = search_div[0].find_all("p")
                with open(file_name, mode='w') as f:
                    for p in ps:
                        print(p.text.replace(" ", "").replace('　　', ''))
                        f.write(p.text.replace(" ", "").replace('　　', '').replace(" ", "") + "\n")


def craw_gov_revenue_expenditure_data():
    for i in range(1):
        if i == 0:
            url = 'https://gks.mof.gov.cn/tongjishuju/index.htm'
        else:
            url = f'https://gks.mof.gov.cn/tongjishuju/index_{i}.htm'
        respond = requests.get(url)
        html = respond.content
        html_doc = str(html, 'utf-8')
        soup = BeautifulSoup(html_doc, 'html.parser')
        uls = soup.find_all("ul", "liBox")
        if uls is not None and len(uls) > 0:
            ul = uls[0]
            a_s = ul.find_all("a")
            if a_s is not None:
                for a_ in a_s:
                    if '财政收支情况' in a_.text:
                        print(a_)
                        detail_url = 'https://gks.mof.gov.cn/tongjishuju' + a_['href'][1:]
                        gov_revenue_expenditure(detail_url)


if __name__ == '__main__':
    #enter_credit_fin()
    enter_credit_fin_agg_flow()
    # handle_balance_sheet_of_monetary_authority(handle_dir='fin_balance_sheet_of_monetary_authority')
    # craw_gov_revenue_expenditure_data()
    get_gov_data()
    # find_data()
