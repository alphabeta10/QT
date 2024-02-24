"""
公共策略
输出：股票新闻情感数据风险，股票价格指标风险，当前宏观风险,股指期货多空比风险，行业风险
"""
from datetime import datetime, timedelta
from indicator.ai_model_indicator import get_model_ai_new_indicator_from_db, get_model_stock_news_analysis_from_db, \
    get_macro_indicator_from_db, get_stock_price_summary_from_db
from indicator.common_indicator import get_last_sz_market_margin_indicator, get_stock_last_dzjy, \
    get_last_sh_market_margin_indicator, get_fin_futures_long_short_rate
import warnings

warnings.filterwarnings('ignore')


def cal_risk_level(risk_value):
    risk_level = '无风险'
    if risk_value > 0 and risk_value < 0.3:
        risk_level = '低风险'
    if risk_value >= 0.3 and risk_value < 0.5:
        risk_level = '中风险'
    if risk_value >= 0.5 and risk_value < 0.8:
        risk_level = "高风险"
    if risk_value >= 0.8:
        risk_level = "特高风险"
    return risk_level


def ai_industry_and_stock_eva_risk(industry_names=None, industry_risk_weight=None, codes=None):
    """
    人工智能相关行业
    :return:
    """
    # 行业风险计算
    if industry_risk_weight is None:
        industry_risk_weight = {"人工智能": 0.5, "算力": 0.5}
    if industry_names is None:
        industry_names = ['人工智能', '算力']
    risk_level_value_dict = common_industry_risk(industry_names,industry_risk_weight)

    # 中性会有0.3的权重风险 悲观是100%的权重
    before30day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    # 股票个股新闻风险计算以及股价波动风险计算
    if codes is None:
        codes = ['300474', '002230', '603019', '000977']
    common_stock_news_and_price_risk(codes,risk_level_value_dict)
    # 大宗交易风险
    dajy_risk_dict = get_stock_last_dzjy(codes, before30day_str)
    # 行业风险 0.5 大宗交易0.2 股票个股风险 0.3
    stock_total_risk = {}
    industr_risk_value = risk_level_value_dict['industry_risk_level']['risk_value'] * 0.2
    market_risk = market_eval_risk()
    for code in codes:
        total_risk = industr_risk_value + market_risk * 0.1
        if code in risk_level_value_dict.keys():
            total_risk += risk_level_value_dict[code]['risk_value'] * 0.5
        if code in dajy_risk_dict.keys():
            total_risk += dajy_risk_dict.get(code)['risk_value'] * 0.2
        stock_total_risk[code] = total_risk
    stock_total_risk['industry_risk'] = risk_level_value_dict['industry_risk_level']['risk_value']
    stock_total_risk['makert_risk'] = market_risk
    return stock_total_risk

def common_stock_total_risk(industry_names=None, industry_risk_weight=None, codes=None):
    risk_level_value_dict = {}
    if industry_risk_weight is not None and industry_names is not None:
        risk_level_value_dict = common_industry_risk(industry_names,industry_risk_weight)

    # 中性会有0.3的权重风险 悲观是100%的权重
    before30day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    # 股票个股新闻风险计算以及股价波动风险计算
    dajy_risk_dict = {}
    if codes is not None:
        common_stock_news_and_price_risk(codes,risk_level_value_dict)
        dajy_risk_dict = get_stock_last_dzjy(codes, before30day_str)
    # 大宗交易风险
    # 行业风险 0.5 大宗交易0.2 股票个股风险 0.3
    stock_total_risk = {}
    industr_risk_value = 0
    if 'industry_risk_level' in risk_level_value_dict.keys():
        industr_risk_value = risk_level_value_dict['industry_risk_level']['risk_value'] * 0.2
        stock_total_risk['industry_risk'] = risk_level_value_dict['industry_risk_level']['risk_value']
    market_risk = market_eval_risk()
    if codes is not None:
        for code in codes:
            total_risk = industr_risk_value + market_risk * 0.1
            if code in risk_level_value_dict.keys():
                total_risk += risk_level_value_dict[code]['risk_value'] * 0.5
            if code in dajy_risk_dict.keys():
                total_risk += dajy_risk_dict.get(code)['risk_value'] * 0.2
            stock_total_risk[code] = total_risk
    stock_total_risk['makert_risk'] = market_risk
    return stock_total_risk

def common_stock_news_and_price_risk(codes,risk_level_value_dict:dict):
    """
    股票新闻以及价格波动风险
    :param codes:
    :param risk_level_value_dict:
    :return:
    """
    before30day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    stock_sentiment_dict = get_model_stock_news_analysis_from_db(codes, before30day_str)
    stock_price_summary_dict = get_stock_price_summary_from_db(codes)
    for code, val in stock_sentiment_dict.items():
        total = sum(list(val.values()))
        rate = {k: v / total for k, v in val.items()}
        ele_stock_risk = stock_price_summary_dict[code]['risk_value'] * 0.3
        for sent, r in rate.items():
            if sent == '中性':
                ele_stock_risk += r * 0.2
            if sent == '悲观':
                ele_stock_risk += r
        risk_level = cal_risk_level(ele_stock_risk)
        risk_level_value_dict[code] = {"risk_level": risk_level, "risk_value": round(ele_stock_risk, 4)}


def common_industry_risk(industry_names=None, industry_risk_weight=None):
    """
    行业风险
    :param industry_names: 行业名称
    :param industry_risk_weight:行业风险权重
    :return:
    """
    risk_level_value_dict = {}

    # 行业风险权重
    if industry_risk_weight is None:
        industry_risk_weight = {"人工智能": 0.5, "算力": 0.5}
    if industry_names is None:
        industry_names = ['人工智能', '算力']
    #中性会有0.3的权重风险 悲观是100%的权重
    before30day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    dict_data = get_model_ai_new_indicator_from_db(industry_names, before30day_str)
    industry_sentiment_st = {}
    industry_risk = 0
    for k, v in dict_data.items():
        total = sum(list(v.values()))
        rate = {sent: round(st / total, 4) for sent, st in v.items()}
        ele_industry_risk = 0
        for sent, r in rate.items():
            if sent == '中性':
                ele_industry_risk += r * 0.3
            if sent == '悲观':
                ele_industry_risk += r
        ele_industry_risk = ele_industry_risk * industry_risk_weight.get(k)
        industry_risk += ele_industry_risk
        industry_sentiment_st[k] = rate

    risk_level = cal_risk_level(industry_risk)
    risk_level_value_dict['industry_risk_level'] = {"risk_level": risk_level, "risk_value": round(industry_risk, 4)}
    return risk_level_value_dict


def get_macro_risk():
    last_sentiment = get_macro_indicator_from_db()
    risk_level = '低风险'
    risk_value = 0.1
    if last_sentiment is not None and last_sentiment == '中性':
        risk_level = '中风险'
        risk_value = 0.5
    if last_sentiment is not None and last_sentiment == '悲观':
        risk_level = "高风险"
        risk_value = 0.8
    return risk_level, risk_value


def market_eval_risk():
    """
    市场风险 可以作为整体仓位管理参考
     整体仓位比例 = 1-风险
    :return:
    """
    total_risk_value = 0
    # 股指期货多空比风险 占比0.3
    fin_futures_risk = get_fin_futures_long_short_rate()
    high_risk, low_risk = fin_futures_risk['high_risk'], fin_futures_risk['low_risk']
    total_risk_value += (high_risk * 0.5 + low_risk * 0.1) * 0.3
    # 宏观风险 占比 0.5
    macro_risk, macro_risk_value = get_macro_risk()
    total_risk_value += macro_risk_value * 0.5
    # 融资融券风险 0.2
    _, last_risk_dict = get_last_sz_market_margin_indicator()
    sz_risk_value = last_risk_dict.get("有风险", 0)

    _, last_risk_dict = get_last_sh_market_margin_indicator()
    sh_risk_value = last_risk_dict.get("有风险", 0)

    total_risk_value += (sz_risk_value + sh_risk_value) * 0.2
    return total_risk_value


if __name__ == '__main__':
    dict_codes = {"002555": "三七互娱",
            "002602": "世纪华通",
            "603444": "吉比特",}
    result = ai_industry_and_stock_eva_risk(codes=list(dict_codes.keys()))
    print(result)
