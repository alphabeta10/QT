import pandas as pd
from utils.actions import show_data
import akshare as ak
from utils.tool import get_data_from_mongo, sort_dict_data_by
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def handle_future_data():
    # symbols = ak.futures_hq_subscribe_exchange_symbol()
    # print(symbols)

    futures_foreign_hist_df = ak.futures_foreign_hist(symbol="S")
    show_data(futures_foreign_hist_df)
    futures_foreign_hist_df = futures_foreign_hist_df[['date','close']]
    futures_foreign_hist_df.set_index(keys='date',inplace=True)
    futures_foreign_hist_df = futures_foreign_hist_df.resample("M").mean()


    futures_foreign_hist_df.plot(kind='line', title='豆类', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def get_goods_data(codes=None):
    database = 'stock'
    collection = 'goods'
    projection = {'_id': False, "name": True, "time": True, "value": True}
    sort_key = "time"
    if codes is None:
        codes = ['大豆']
    condition = {"name": {"$in": codes}, "data_type": "goods_price"}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    return data


def analysis_gooods_price():
    pd_data = get_goods_data(codes=['大豆', '豆粕','大豆油'])
    pd_data['time'] = pd.to_datetime(pd_data['time'])
    pd_data = pd.pivot_table(pd_data, index='time', values='value', columns='name')
    print(pd_data)
    pd_data = pd_data.resample("M").mean()
    for index in pd_data.index:
        dict_data = dict(pd_data.loc[index])
        index = str(index)
        print(index,dict_data)
    #print(pd_data)
    pd_data.plot(kind='line', title='豆类', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def find_area_code():
    codes = pd.read_csv(
        "/Users/alpha/Downloads/Production_Crops_Livestock_E_All_Data/Production_Crops_Livestock_E_AreaCodes.csv",
        encoding='latin-1')
    print(codes)
    state_list = []
    for index in codes.index:
        ele = dict(codes.loc[index])
        acode = ele['Area Code']
        if len(str(acode)) == 4:
            mcode = ele['M49 Code']
            area = ele['Area']
        else:
            mcode = ele['M49 Code']
            area = ele['Area']
            state_list.append(mcode)
    print(state_list)


def get_main_country_production():
    file_name = '/Users/alpha/Downloads/Production_Crops_Livestock_E_All_Data/Production_Crops_Livestock_E_All_Data.csv'
    pd_data = pd.read_csv(file_name, encoding='latin-1')
    """
    Barley 大麦  俄罗斯，澳大利亚，法国，德国
    Soya beans 大豆 巴西，美国，阿根廷
    Maize (corn) 玉米 美国，中国，巴西，阿根廷
    Sorghum 高粱 美国，印度
    Wheat 小麦 中国，印度，俄罗斯，美国
    Rice 稻谷 中国，印度
    """
    item_code = {"Rice": "稻谷", "Wheat": "小麦", "Sorghum": "高粱", "Maize (corn)": "玉米", "Soya beans": "大豆",
                 "Barley": "大麦"}
    get_area_codes = ["'004", "'008", "'012", "'024", "'028", "'032", "'051", "'036", "'040", "'031", "'044", "'048",
                      "'050", "'052", "'112", "'056", "'058", "'084", "'204", "'064", "'068", "'070", "'072", "'076",
                      "'096", "'100", "'854", "'108", "'132", "'116", "'120", "'124", "'140", "'148", "'152", "'159",
                      "'344", "'446", "'156", "'158", "'170", "'174", "'178", "'184", "'188", "'384", "'191", "'192",
                      "'196", "'203", "'200", "'408", "'180", "'208", "'262", "'212", "'214", "'218", "'818", "'222",
                      "'226", "'232", "'233", "'748", "'231", "'230", "'234", "'242", "'246", "'250", "'254", "'258",
                      "'266", "'270", "'268", "'276", "'288", "'300", "'308", "'312", "'320", "'324", "'624", "'328",
                      "'332", "'340", "'348", "'352", "'356", "'360", "'364", "'368", "'372", "'376", "'380", "'388",
                      "'392", "'400", "'398", "'404", "'296", "'414", "'417", "'418", "'428", "'422", "'426", "'430",
                      "'434", "'440", "'442", "'450", "'454", "'458", "'462", "'466", "'470", "'584", "'474", "'478",
                      "'480", "'484", "'583", "'496", "'499", "'504", "'508", "'104", "'516", "'520", "'524", "'528",
                      "'540", "'554", "'558", "'562", "'566", "'570", "'807", "'578", "'512", "'586", "'275", "'591",
                      "'598", "'600", "'604", "'608", "'616", "'620", "'630", "'634", "'410", "'498", "'638", "'642",
                      "'643", "'646", "'659", "'662", "'670", "'882", "'678", "'682", "'686", "'688", "'891", "'690",
                      "'694", "'702", "'703", "'705", "'090", "'706", "'710", "'728", "'724", "'144", "'729", "'736",
                      "'740", "'752", "'756", "'760", "'762", "'764", "'626", "'768", "'772", "'776", "'780", "'788",
                      "'792", "'795", "'798", "'800", "'804", "'784", "'826", "'834", "'840", "'858", "'810", "'860",
                      "'548", "'862", "'704", "'887", "'890", "'894", "'716"]

    for k, cname in item_code.items():
        print(f"name={cname}")
        data = pd_data[
            ((pd_data['Item'] == k) & (pd_data['Element'] == 'Production') & (
                pd_data['Area Code (M49)'].isin(get_area_codes)))]
        data = data[['Area Code', 'Area', 'Y2021']]
        data.dropna(inplace=True)
        data.sort_values(by="Y2021", inplace=True)
        show_data(data.tail(10))


if __name__ == '__main__':
    macro_euro_lme_holding_df = ak.macro_euro_lme_stock()
    show_data(macro_euro_lme_holding_df)
