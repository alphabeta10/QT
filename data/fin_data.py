import pandas as pd
import os
import re
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
    result = re.findall(name + "(\d+\.?\d?)亿元，同比增长(\d+\.?\d?)%", raw_line)
    if len(result) > 0:
        first, other = result[0]
        return first, float(other) * 1
    else:
        result = re.findall(name + "(\d+\.?\d?)亿元，同比下降(\d+\.?\d?)%", raw_line)
        if len(result) > 0:
            first, other = result[0]
            return first, float(other) * -1
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
        "国有土地使用权出让收入相关支出": "state_owned_land_expenditure",
    }

    all_values = list(gov_expenditure_config.values()) + list(gov_fund_income_config.values()) + list(
        tax_config.values()) + list(detail_expenditure_config.values()) + list(all_expenditure_config.values()) + list(
        all_income_config.values())
    with open(filename, mode='r') as f:
        lines = f.readlines()
        for line in lines:
            if '全国一般公共预算收入' in line:
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
            if "全国一般公共预算支出" in line:
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
            if "全国政府性基金预算收入" in line:
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
            if "全国政府性基金预算支出" in line:
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
            print(key)
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
    print(file_list)
    update_request = []
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for file_name in file_list:
        file_path = os.path.join(dir, file_name)
        dict_data = gks_fin_data(file_path)
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
        "(事)业单位贷款":"loans_to_non_financial_enterprises_and_government_departments_organizations",
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
            "人民币贷款":"rmb_loans",
            "外币贷款（折合人民币":"foreign_currency_loans",
            "委托贷款":"entrusted_loans",
            "信托贷款":"trust_loans",
            "未贴现银行承兑汇票":"undiscounted_banker_acceptances",
            "企业债券":"net_fin_cor_bonds",
            "政府债券":"gov_bonds",
            "非金融企业境内股票":"equity_stock_non_fin_enter",
            "存款类金融机构资产支持证券":"asset_backed_fin_inst",
            "贷款核销":"loans_written_off",
        }
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            name = str(dict_data['社会融资规模存量统计表']).strip().replace(" ","")
            if name in agg_stock_dict.keys():
                col_name = agg_stock_dict[name]
                for i in range(1,13):
                    first_index = i*2 -1
                    value = str(dict_data[f'Unnamed: {first_index}'])
                    second_index = i*2
                    stock_percent = str(dict_data[f'Unnamed: {second_index}'])
                    if i<10:
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
            "社会融资规模存量":"afre",
            "人民币贷款":"rmb_loans",
            "外币贷款（折合人民币":"foreign_currency_loans",
            "委托贷款":"entrusted_loans",
            "信托贷款":"trust_loans",
            "未贴现银行承兑汇票":"undiscounted_banker_acceptances",
            "企业债券":"net_fin_cor_bonds",
            "政府债券":"gov_bonds",
            "非金融企业境内股票":"equity_stock_non_fin_enter",
            "存款类金融机构资产支持证券":"asset_backed_fin_inst",
            "贷款核销":"loans_written_off",
        }
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            name = str(dict_data['社会融资规模存量统计表']).strip().replace(" ","")
            if name in agg_stock_dict.keys():
                col_name = agg_stock_dict[name]
                for i in range(1,13):
                    first_index = i*2 -1
                    value = str(dict_data[f'Unnamed: {first_index}'])
                    second_index = i*2
                    stock_percent = str(dict_data[f'Unnamed: {second_index}'])
                    if i<10:
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



def enter_credit_fin():
    file_dir = 'fin_credit_funds2023'
    type_dict_mapping = {'金融机构外汇信贷收支表': 'fin_inst_foreign',
                         '金融机构本外币信贷收支表': 'fin_inst_rmb_foreign', '金融机构人民币信贷收支表': "fin_inst_rmb"}
    for key, v in type_dict_mapping.items():
        print(key)
        fin_ins_credit_funds_data(file_dir=file_dir, type_name=key, time='2023')

def enter_credit_fin_agg_flow():
    file_dir = 'fin_agg2023'
    type_dict_mapping = {'社会融资规模增量统计表': 'agg_fin_flow',
                         '社会融资规模存量统计表': 'agg_fin_stock'}
    for key, v in type_dict_mapping.items():
        handle_credit_agg_stock_data(file_dir=file_dir, type_name=key, time='2023')

if __name__ == '__main__':
    enter_credit_fin()
    enter_credit_fin_agg_flow()
    find_data()
