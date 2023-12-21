import pymongo
def get_mongo_table(url='mongodb://localhost:27017/',database='stock',collection='ticker_daily'):
    """
    获取mongo数据报表
    :param url: mongo url
    :param database: mongo 数据库
    :param collection: mongo 集合数据
    :return:
    """
    client = pymongo.MongoClient(url)
    stock = client[database]
    col = stock[collection]
    return col

if __name__ == '__main__':
    pass