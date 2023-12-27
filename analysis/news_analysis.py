from data.mongodb import get_mongo_table
from datetime import datetime, timedelta
from utils.tool import sort_dict_data_by
from utils.actions import show_data
import numpy as np
import jieba.analyse
from sklearn.cluster import KMeans
import jieba.posseg as pseg
from gensim.models.doc2vec import TaggedDocument, Doc2Vec


def word_in_sentence(word_list, sentence):
    for word in word_list:
        if word in sentence:
            return True, word
    return False, None


def read_file(file_name):
    with open(file_name, mode='r', encoding='utf8') as f:
        lines = f.readlines()
        for i in range(len(lines)):
            lines[i] = lines[i].rstrip("\n")
        return lines


def cut_word_st(dict_word_count, sentence, stopwords=None):
    words = jieba.cut(sentence, use_paddle=True)
    set_words = set()
    for i in range(len(words)):
        set_words.add(words[i])
    for word in set_words:
        if stopwords is not None:
            if word not in stopwords:
                if word not in dict_word_count.keys():
                    dict_word_count[word] = 0
                dict_word_count[word] += 1
        else:
            if word not in dict_word_count.keys():
                dict_word_count[word] = 0
            dict_word_count[word] += 1


def cut_word_pseg_st(dict_word_count, sentence, stopwords=None):
    words = pseg.cut(sentence, use_paddle=True)
    set_words = set()
    for word, flag in words:
        # if flag[0] == 'n':
        set_words.add(word)
    for word in set_words:
        if stopwords is not None:
            if word not in stopwords:
                if word not in dict_word_count.keys():
                    dict_word_count[word] = 0
                dict_word_count[word] += 1
        else:
            if word not in dict_word_count.keys():
                dict_word_count[word] = 0
            dict_word_count[word] += 1


def analysis_news():
    goods = get_mongo_table(database='stock', collection='news')
    stop_file_name = "stopwords.dat"
    stopwords = read_file(stop_file_name)

    date_time = datetime.now() - timedelta(days=365)
    today = date_time.strftime("%Y-%m-%d")
    dict_key_words = {"机器人": 0, "算力": 0, "人工智能": 0, "通信": 0, "光伏": 0, "风电": 0, "新能源": 0, "工信部": 0,
                      "宁德时代": 0, "氢能": 0}
    key_words = dict_key_words.keys()
    dict_word_count = {}
    for ele in goods.find({"data_type": "cls_telegraph", "time": {"$gt": f"{today}"}}, projection={'_id': False}).sort(
            "time"):
        sentence = ele['content']
        title = ele['title']
        if "工信部" in title:
            time = ele['time']
            cut_word_pseg_st(dict_word_count, sentence, stopwords=stopwords)
            is_ture, word = word_in_sentence(key_words, sentence)
            if is_ture:
                print("*" * 50)
                print(word, title, sentence, time)
                dict_key_words[word] += 1
                print("*" * 50)
    print(sort_dict_data_by(dict_key_words, by='value'))
    print(sort_dict_data_by(dict_word_count, by='value'))

    for k, v in sort_dict_data_by(dict_word_count, by='value').items():
        if k in dict_key_words.keys():
            print(k, v)

        # is_pass,key_word  = word_in_sentence(key_words,sentence)
        # if is_pass:
        #     print(key_word,title,time)
        # if '工信部' in sentence:
        #     print(sentence)
    #     if '营业收入' not in sentence:
    #         time = ele['time']
    #         print("*" * 50)
    #         print(sentence)
    #
    #         split_sentence = sentence.split("】")
    #         if len(split_sentence)>1:
    #             sentence = split_sentence[1]
    #         key_words_top = jieba.analyse.textrank(sentence, topK=10, withWeight=False)
    #         print(time+"="+"/".join(key_words_top))
    #         for word in key_words_top:
    #             if word not in datas.keys():
    #                 datas[word] = 0
    #             datas[word] += 1
    #
    #         # key_words_top = jieba.analyse.extract_tags(sentence)
    #         # print(time + "=" + "/".join(key_words_top))
    #         print("*"*50)
    # print(sorted(datas.items(),key=lambda x:(x[1],x[0]),reverse=True))


def analysis_gxb_news():
    goods = get_mongo_table(database='stock', collection='news')
    stop_file_name = "stopwords.dat"
    stopwords = read_file(stop_file_name)

    date_time = datetime.now() - timedelta(days=365)
    today = date_time.strftime("%Y-%m-%d")
    dict_key_words = {"工信部": 0, "发改委": 0, "天气": 0}
    dict_key_words = {"大豆":0,"巴西":0}
    key_words = dict_key_words.keys()
    dict_word_count = {}
    for ele in goods.find({"data_type": "cls_telegraph", "time": {"$gt": f"{today}"}}, projection={'_id': False}).sort(
            "time"):
        sentence = ele['content']
        title = ele['title']
        if "巴西" in title and '大豆' in title:
            time = ele['time']
            cut_word_pseg_st(dict_word_count, sentence, stopwords=stopwords)
            is_ture, word = word_in_sentence(key_words, sentence)
            if is_ture:
                print("*" * 50)
                print(word, title, sentence, time)
                dict_key_words[word] += 1
                print("*" * 50)
    print(sort_dict_data_by(dict_key_words, by='value'))
    print(sort_dict_data_by(dict_word_count, by='value'))

    for k, v in sort_dict_data_by(dict_word_count, by='value').items():
        if k in dict_key_words.keys():
            print(k, v)


def stock_new_tmp():
    import akshare as ak
    print(ak.__version__)
    stock_news_em_df = ak.stock_news_em(symbol="300015")
    text_list = []
    for index in stock_news_em_df.index:
        print(dict(stock_news_em_df.loc[index]))
        new_content = stock_news_em_df.loc[index]['新闻内容']
        text_list.append(new_content)
    cut_sentence_list = cut_sentence(text_list)
    train_data = X_train(cut_sentence_list)
    model = train(train_data)
    words = ['月', '28', '日', '爱尔', '眼科', '公告', '公司', '拟以', '亿', '亿元', '回购', '股份', '用于', '实施',
             '股权', '激励', '计划', '员工', '持股', '计划', '回购', '价不超', '44.16', '元']
    inferred_vector = model.infer_vector(doc_words=words,alpha=0.025,epochs=500)
    print(inferred_vector)

    sims = model.docvecs.most_similar([inferred_vector],topn=10)

    for count,sim in sims:
        print(count,sim)
        sentence = text_list[count]
        words = ''
        for word in sentence:
            words = words + word + " "
        print(words,sim,len(sentence))


    # 新闻关键词 减持   增持   减少 质押


def cluster_model():
    X = np.array([[1,2],[1,4],[4,4]])
    kmeans = KMeans(n_clusters=2,random_state=0).fit(X)
    print(kmeans)


def cut_sentence(text_list):
    # 加载停用词
    stop_file_name = "stopwords.dat"
    stopwords = read_file(stop_file_name)
    result = []
    for each in text_list:
        each_cut = jieba.cut(each)
        each_split = " ".join(each_cut).split()
        each_result = [word for word in each_split if word not in stopwords]
        result.append(' '.join(each_result))
    return result


def X_train(cut_sentence):
    x_train = []
    for i, text in enumerate(cut_sentence):
        word_list = text.split(' ')
        l = len(word_list)
        word_list[l - 1] = word_list[l - 1].strip()
        document = TaggedDocument(word_list, tags=[i])
        x_train.append(document)
    return x_train


def train(x_train, size=300):
    model = Doc2Vec(x_train, min_count=1, window=3, vector_size=size, sample=1e3,negative=5,workers=4)
    model.train(x_train, total_examples=model.corpus_count, epochs=10)
    return model


if __name__ == '__main__':
    analysis_gxb_news()
