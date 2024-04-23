import numpy as np
import pandas as pd
from data.mongodb import get_mongo_table
from utils.actions import show_data
import matplotlib.pyplot as plt
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')
from data.stock_detail_fin import handle_comm_stock_fin_em,handle_fin_analysis_indicator
import math



def get_stock_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False})
    new_codes = {}
    for ticker in tickers_cursor:
       new_codes[ticker['symbol']] = ticker['industry']
       new_codes["name"] = ticker['name']
    return new_codes

def get_data(cods=None,dtype="fin_indicator"):
    if cods is None:
        cods = ['002709', '600884','002015','000422','600096']
        cods = ['603288','601009','600036','002507','002385','603363']
        #cods = ['601009']
    fin_col = get_mongo_table(collection='fin')
    ret = fin_col.find({"code": {"$in":cods},"data_type":dtype},projection={'_id': False}).sort("date")
    datas = []
    for ele in ret:
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    return pd_data


def get_all_data(dates=None,dtype="fin_indicator"):
    if dates is None:
        dates = ['20221231', '20211231']
    fin_col = get_mongo_table(collection='fin')
    ret = fin_col.find({"date": {"$in":dates},"data_type":dtype},projection={'_id': False}).sort("date")
    datas = []
    for ele in ret:
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    return pd_data


def analysis_01():
    def convert_ele(ele):
        if ele=='--':
            ele = 0
        return ele
    get_col = ['销售毛利率(%)','营业利润率(%)','code','date','净利润增长率(%)']
    pd_data = get_data()
    print(pd_data.columns)
    pd_data = pd_data[get_col]
    pd_data['销售毛利率(%)'] = pd_data['销售毛利率(%)'].apply(convert_ele)
    pd_data['营业利润率(%)'] = pd_data['营业利润率(%)'].apply(convert_ele)
    pd_data['净利润增长率(%)'] = pd_data['净利润增长率(%)'].apply(convert_ele)
    show_data(pd_data)
    pd_data = pd.pivot_table(pd_data,values='净利润增长率(%)', index=['date'], columns=['code']).tail(10)
    print(pd_data)
    pd_data.plot(kind='bar', title='成本分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()



def analysis_sale_rate():
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        return ele

    code_dict = {"600132":"重庆啤酒","600600":"青岛啤酒","002461":"珠江啤酒"}
    code_dict = {"002459":"晶澳科技","601012":"隆基绿能","603806":"福斯特","605117":"德业股份"}
    pd_data = get_data(dtype='yjbb',cods=list(code_dict.keys()))
    print(pd_data.columns)
    pd_data = pd_data
    pd_data['gross_profit_ratio'] = pd_data['gross_profit_ratio'].apply(convert_ele)
    pd_data['net_profit_cycle_in'] = pd_data['net_profit_cycle_in'].apply(convert_ele)
    pd_data['total_revenue_cylce_in'] = pd_data['total_revenue_cylce_in'].apply(convert_ele)
    show_data(pd_data)

    data = pd.pivot_table(pd_data, values='gross_profit_ratio', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=code_dict)
    print(data)
    #pd_data.plot(kind='bar', title='销售毛利率分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    data.plot(kind='line', title='销售毛利率分析', rot=45, figsize=(15, 8), fontsize=10)
    #plt.show()

    data = pd.pivot_table(pd_data, values='net_profit_cycle_in', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=code_dict)
    print(data)
    # pd_data.plot(kind='bar', title='销售毛利率分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    data.plot(kind='line', title='净利润同比增长', rot=45, figsize=(15, 8), fontsize=10)
    #plt.show()

    data = pd.pivot_table(pd_data, values='total_revenue_cylce_in', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=code_dict)
    print(data)
    # pd_data.plot(kind='bar', title='销售毛利率分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    data.plot(kind='line', title='营业同比增长', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()







def analysis_growth():
    """
    运营能力分析框架
    存货周转次数
    偿债能力
        短期偿债能力
            流动比率
            速动比率
            现金比率
        长期偿债能力
            资产负债率
            权益乘数
    盈利能力
        净资产收益率
        总资产收益率
        每股收益
        市盈率

    :return:
    """
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        return ele
    get_col = ['存货周转率(次)','应收账款周转率(次)','流动资产周转率(次)','总资产周转率(次)','code','date']
    get_col = ['流动比率','现金比率(%)','速动比率','资产负债率(%)','股东权益与固定资产比率(%)','利息支付倍数','code','date']
    get_col = ['净资产收益率(%)','总资产净利润率(%)','每股收益_调整后(元)','code','date']

    get_col = ['净资产收益率(%)','主营业务收入增长率(%)','净资产增长率(%)','总资产增长率(%)']

    code_dict = {"002459":"晶澳科技","601012":"隆基绿能","603806":"福斯特","605117":"德业股份"}
    code_dict = {"002459":"晶澳科技","601012":"隆基绿能","603806":"福斯特","605117":"德业股份"}

    code_dict = {"600132":"重庆啤酒","600600":"青岛啤酒","002461":"珠江啤酒","002507":"涪陵榨菜"}
    code_dict = {"600989":"宝丰能源"}
    code_dict = {"603288":"海天味业"}
    code_dict = {"600884":"杉杉股份"}
    code_dict = {"600111":"北方稀土","002709":"天赐材料"}
    code_dict = {"600519":"贵州茅台"}
    code_dict = {"601168":"西部矿业"}

    #pd_data = get_data()[get_col]
    handle_fin_analysis_indicator(codes=code_dict.keys())
    pd_data = get_data(cods=list(code_dict.keys()))
    if pd_data.empty:
        handle_fin_analysis_indicator(codes=['002466'])
        pd_data = get_data(cods=['002466'])
    pd_data = pd_data[pd_data['date'].str.contains("0331")]
    for cov_col in get_col:
        pd_data[cov_col] = pd_data[cov_col].apply(convert_ele)
    #show_data(pd_data)
    for col in get_col[0:3]:
        new_pd_data = pd.pivot_table(pd_data, values=col, index=['date'], columns=['code']).tail(50)
        new_pd_data = new_pd_data.rename(columns=code_dict)
        print(new_pd_data)
        new_pd_data.plot(kind='bar', title=col, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
        #new_pd_data.plot(kind='line', title='销售毛利率分析', rot=45, figsize=(15, 8), fontsize=10)
        plt.show()

def analysis_assert():
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        return ele

    pd_data = get_data(dtype='zcfz',cods=['000063'])
    print(pd_data.columns)
    get_col = ['money_cap','inventories','lia_acct_payable','lia_acct_receiv']
    for col in get_col:
        pd_data[col] = pd_data[col].apply(convert_ele)
    pd_data = pd_data[pd_data['date'].str.contains("0331")]
    get_col.append('date')
    show_data(pd_data)
    pd_data = pd_data[get_col]
    pd_data.set_index(keys='date',inplace=True)
    pd_data.plot.area()
    plt.show()

def analysis_detail_assert():
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        return float(ele) / 1e8
    #

    pd_data = get_data(dtype='zcfz_report_detail', cods=['002466'])
    if pd_data.empty:
        handle_comm_stock_fin_em(codes=['sz002466'],data_type="zcfz_report_detail")
        pd_data = get_data(dtype='zcfz_report_detail', cods=['002466'])
    get_col_dict = {"MONETARYFUNDS":"货币资金",
                    "INVENTORY":"存货",
                    "ACCOUNTS_RECE":"应收票据及应收账款",
                    "PREPAYMENT":"预付款项",
                    }
    for col in get_col_dict.keys():
        pd_data[col] = pd_data[col].apply(convert_ele)


    pd_data = pd_data[pd_data['date'].str.contains("03-31")]
    # get_col.append('date')
    get_col = list(get_col_dict.keys())
    get_col.append("date")
    pd_data = pd_data[get_col].rename(columns=get_col_dict)
    show_data(pd_data)
    pd_data.set_index(keys='date', inplace=True)
    pd_data.plot.area()
    plt.show()



def analysis_detail_cash_flow():
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        if float(ele)<0:
            return 0
        return float(ele) / 1e8
    #
    pd_data = get_data(dtype='cash_flow_report_em_detail', cods=['688019'])
    if pd_data.empty:
        handle_comm_stock_fin_em(codes=['sh688019'],data_type="cash_flow_report_em_detail")
        pd_data = get_data(dtype='cash_flow_report_em_detail', cods=['688019'])
    get_col_dict = {"NETCASH_OPERATE":"经营活动产生的现金流量净额",
                    "TOTAL_OPERATE_OUTFLOW":"经营活动现金流出小计",
                    "TOTAL_OPERATE_INFLOW":"经营活动现金流入小计",
                    "SALES_SERVICES":"销售商品、提供劳务收到的现金",
                    "BUY_SERVICES":"购买商品、接受劳务支付的现金",
                    "PAY_STAFF_CASH":"支付给职工以及为职工支付的现金",
                    }
    for col in get_col_dict.keys():
        pd_data[col] = pd_data[col].apply(convert_ele)
    pd_data = pd_data[pd_data['date'].str.contains("03-31")]
    get_col = list(get_col_dict.keys())
    get_col.append("date")
    pd_data = pd_data[get_col].rename(columns=get_col_dict)
    show_data(pd_data)
    pd_data.set_index(keys='date', inplace=True)
    pd_data.plot.area()
    plt.show()



def analysis_detail_zcfc():
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        if float(ele)<0:
            return 0
        return float(ele) / 1e8
    code_dict = {"sz002459":"晶澳科技","sh601012":"隆基绿能","sh603806":"福斯特","sh605117":"德业股份"}
    code_dict = {"002459":"晶澳科技","601012":"隆基绿能","603806":"福斯特","605117":"德业股份"}
    code_dict = {"sz002709":"天赐材料"}
    #handle_comm_stock_fin_em(codes=list(code_dict.keys()),data_type="zcfz_report_detail")
    code_dict = {"002709":"天赐材料"}
    code_dict = {"002459":"晶澳科技","601012":"隆基绿能","603806":"福斯特","605117":"德业股份"}




    pd_data = get_data(dtype='zcfz_report_detail', cods=list(code_dict.keys()))
    get_col_dict = {"MONETARYFUNDS": "货币资金",
                    "INVENTORY": "存货",
                    "ACCOUNTS_RECE": "应收票据及应收账款",
                    "PREPAYMENT": "预付款项",
                    "TOTAL_ASSETS":"资产总计",
                    "TOTAL_LIABILITIES":"负债合计",
                    "INTANGIBLE_ASSET":"无形资产",
                    }
    pd_data = pd_data[pd_data['date'].str.contains("03-31")]
    pd_data['book_value'] = pd_data['TOTAL_ASSETS'].astype(float)-pd_data['TOTAL_LIABILITIES'].astype(float)-pd_data['INTANGIBLE_ASSET'].astype(float)

    get_col = list(get_col_dict.keys())
    get_col.append("date")

    data = pd.pivot_table(pd_data, values='book_value', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=code_dict)
    #print(data)
    # pd_data.plot(kind='bar', title='销售毛利率分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    #data.plot(kind='line', title='账面价值', rot=45, figsize=(15, 8), fontsize=10)
    #plt.show()

    data = pd.pivot_table(pd_data, values='MONETARYFUNDS', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=code_dict)

    new_pct_list = []
    for key,v in code_dict.items():
        new_col = f"{v}_inventory_pct"
        new_pct_list.append(new_col)
        data[new_col] = data[v].pct_change(1).round(4)
    # pd_data.plot(kind='bar', title='销售毛利率分析', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    data[code_dict.values()].plot(kind='bar', title='货币资金', rot=45, figsize=(15, 8), fontsize=10)
    plt.legend(loc='upper left')
    data[new_pct_list].plot(kind='bar', title='货币资金同比', rot=45, figsize=(15, 8), fontsize=10)
    plt.legend(loc='upper right')
    plt.show()


def plot_bar_line_list(x, bar_y_list, line_z_list, bar_label_list, line_label_list,title_name):
    # 绘制柱图
    for bar_y,bar_label in zip(bar_y_list,bar_label_list):
        plt.bar(x=x, height=bar_y, label=bar_label, alpha=0.8)
    # 在左侧显示图例
    plt.legend(loc='upper left')
    # 设置标题
    plt.title(title_name)
    plt.xlabel("日期")
    plt.ylabel("数值")

    # 画折线图
    ax2 = plt.twinx()
    ax2.set_ylabel("同比")
    # 设置y坐标范围
    # ax2.set_ylim([-100,100])
    for line_z,line_label in zip(line_z_list,line_label_list):
        plt.plot(x, line_z, linewidth='1', label=line_label)
    # 显示数字
    for line_z in line_z_list:
        for a, b in zip(x, line_z):
            plt.text(a, b, b, ha='center', va='bottom', fontsize=8)
    plt.legend(loc="upper right")
    plt.show()

def plot_bar_line(x,bar_y,line_z,bar_label,line_label):
     #绘制柱图
     plt.bar(x=x,height=bar_y,label=bar_label,color='Coral',alpha=0.8)
     #在左侧显示图例
     plt.legend(loc='upper left')
     #设置标题
     plt.title("Detection results")
     plt.xlabel("日期")
     plt.ylabel("数值")

     #画折线图
     ax2 = plt.twinx()
     ax2.set_ylabel("同比")
     #设置y坐标范围
     #ax2.set_ylim([-100,100])
     plt.plot(x,line_z,"r",marker='.',c='r',ms=5,linewidth='1',label=line_label)
     #显示数字
     for a,b in zip(x,line_z):
         plt.text(a,b,b,ha='center',va='bottom',fontsize=8)

     plt.legend(loc="upper right")
     plt.show()

def import_googds_yd(goods_name='大麦'):
    data_info = get_mongo_table(database='govstats', collection='customs_goods')
    datas = []
    for ele in data_info.find({"name":goods_name,"data_type":"import_goods_detail","unit":"万吨"},projection={'_id': False}).sort("date"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    dict_fs = {
        "month_volume_cyc":"当月数量同比",
        "month_amount_cyc":"当月金额同比",
        "acc_month_amount_cyc":"累计当月金额同比",
        "acc_month_volume_cyc":"累计当月数量同比",
    }
    #fs = ['month_volume']
    #data[['month_amount','month_volume']] = data[['month_amount','month_volume']].astype(float)
    data[list(dict_fs.keys())] = data[list(dict_fs.keys())].astype(float)
    data.set_index(keys=['date'],inplace=True)
    data = data.rename(columns=dict_fs)

    #data[['import_amount','export_amount']].plot(kind='bar',title='export car vol')
    data[list(dict_fs.values())].plot(kind='bar',title=goods_name,figsize=(15,8),rot=45)
    plt.show()


def common_board_googds_yd(get_dict=None):
    if get_dict is None:
        get_dict = {"name":'大麦', "data_type": "import_goods_detail", "unit": "万吨"}
    goods_name = get_dict['name']
    data_info = get_mongo_table(database='govstats', collection='customs_goods')
    datas = []
    for ele in data_info.find(get_dict,projection={'_id': False}).sort("date"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    dict_fs = {
        "month_volume_cyc":"当月数量同比",
        "month_amount_cyc":"当月金额同比",
        "acc_month_amount_cyc":"累计当月金额同比",
        "acc_month_volume_cyc":"累计当月数量同比",
    }
    #fs = ['month_volume']
    #data[['month_amount','month_volume']] = data[['month_amount','month_volume']].astype(float)
    data[list(dict_fs.keys())] = data[list(dict_fs.keys())].astype(float)
    data.set_index(keys=['date'],inplace=True)
    data = data.rename(columns=dict_fs)

    #data[['import_amount','export_amount']].plot(kind='bar',title='export car vol')
    data[list(dict_fs.values())].plot(kind='bar',title=goods_name,figsize=(15,8),rot=45)
    plt.show()




def source_predict_jd(goods_name='小麦', qc_config=None):
    if qc_config is None:
        qc_config = {"q1": ['20230101', '20230331'], "q2": ['20230401', '20230630']}
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []

    qc = {}
    for ele in goods.find({"name": goods_name, "data_type": "goods_price"}, projection={'_id': False}).sort("time"):
        time = ele['time']
        for k,v in qc_config.items():
            start,end = v
            if int(time)>=int(start) and int(time)<=int(end):
                if k not in qc.keys():
                    qc[k] = []
                qc[k].append(float(ele['value']))
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['time', 'value']]
    data[['value']] = data[['value']].astype(float)
    data.set_index(keys=['time'], inplace=True)
    data[['value']].plot(kind='line', title=goods_name, rot=45, figsize=(15, 8), fontsize=10)
    new_qc = {}
    for k,v in qc.items():
        mean = np.mean(v)
        new_qc[k] = mean
    pct = round((new_qc['q2']-new_qc['q1'])/new_qc['q1'],6)
    q1 = round(new_qc['q1'],2)
    q2 = round(new_qc['q2'],2)
    print(f"q1={q1},q2={q2} pct-change={pct}")
    plt.show()


def source_predict_common(goods_name=None, config=None):
    if config is None:
        config = {"m11": ['20240301', '20240331'], "m12": ['20240401', '20240431']}
        #config = {"m11": ['20230301', '20230331'], "m12": ['20240301', '20240331']}
    keys = list(config.keys())
    print(keys)
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    sc = {}
    if goods_name is None:
        for ele in goods.find({"data_type": "goods_class"},
                              projection={'_id': False, 'data_type': False, 'name': False, 'time': False}).sort("time"):
            meta_dict = ele
        new_meta_dict = {}
        for cls, cols in meta_dict.items():
            for col in cols:
                new_meta_dict[col] = cls
        meta_dict = new_meta_dict
        goods_name = {"$in":list(meta_dict.keys())}
    time_condition = {"$gte":config[keys[0]][0]}
    for ele in goods.find({"name": goods_name, "data_type": "goods_price","time":time_condition}, projection={'_id': False}).sort("time"):
        time = ele['time']
        name = ele['name']
        for k, v in config.items():
            start, end = v
            if int(time) >= int(start) and int(time) <= int(end):
                if name not in sc.keys():
                    sc[name] = {}
                    sc[name][k] = []
                if k not in sc[name].keys():
                    sc[name][k] = []
                sc[name][k].append(float(ele['value']))
        datas.append(ele)
    new_sc = {}
    for name, comm_dict in sc.items():
        for k,v in comm_dict.items():
            if name not in new_sc.keys():
                new_sc[name] = {}
            new_sc[name][k] = np.mean(v)
    keys = list(config.keys())
    count_dict = {"gt":[],"lt":[]}
    for name,comm_dict in new_sc.items():
        if len(list(comm_dict.keys()))==2:
            pct = round((comm_dict[keys[1]] - comm_dict[keys[0]]) / comm_dict[keys[0]], 6)
            s1 = round(comm_dict[keys[0]], 2)
            s2 = round(comm_dict[keys[1]], 2)
            if pct>=0:
                count_dict['gt'].append(name+"="+str(pct))
            else:
                count_dict['lt'].append(name+"="+str(pct))
        else:
            print(name,comm_dict)
    for k,v in count_dict.items():
        print(k,v,len(v))

if __name__ == '__main__':
    #goods_name = '大豆'
    #source_predict_yj(goods_name=goods_name)
    #import_googds_yd(goods_name=goods_name)
    source_predict_common()





