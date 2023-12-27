from analysis.analysis_tool import plot_year_seq_data
from utils.tool import get_data_from_mongo


def analysis_futrues_by_plot_year_data(symbol=None):
    if symbol is None:
        symbol = 'B0'
    database = 'futures'
    collection = 'futures_daily'

    projection = {'_id': False}
    condition = {"symbol": symbol,"date":{"$gt":'2019-01-01',"$lt":"2019-12-31"}}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    plot_year_seq_data(data, index_key='date', val_key='close')


if __name__ == '__main__':
    analysis_futrues_by_plot_year_data()
