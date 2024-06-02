import akshare as ak
from datetime import datetime, timedelta
import pywencai
from pymongo import MongoClient

def is_market_open(date: str, trade_cal_df) -> bool:
    """
    查询指定日期是否为交易日

    :param date: 日期字符串，格式为 'YYYY-MM-DD'
    :param trade_cal_df: 交易日历数据 DataFrame
    :return: 如果是交易日返回 True，否则返回 False
    """
    date_formatted = datetime.strptime(date, '%Y-%m-%d').date()
   # print(f"Checking if {date_formatted} is in trade calendar...")  # 调试信息

    # 判断指定日期是否在交易日历中
    return date_formatted in trade_cal_df['trade_date'].values

def save_to_mongodb(dataframe, date_str):
    """
    将 DataFrame 保存到 MongoDB
    """
    client = MongoClient('mongodb://localhost:27017/')
    db = client['stock_data']
    collection = db['non_suspended_stocks']
    # 添加日期列
    dataframe['date'] = date_str
    # 将 DataFrame 转换为字典并插入 MongoDB
    data_dict = dataframe.to_dict("records")
    collection.insert_many(data_dict)
    print(f"Data for {date_str} has been saved to MongoDB")

if __name__ == "__main__":
    # 获取交易日历数据
    trade_cal_df = ak.tool_trade_date_hist_sina()

    # 打印交易日历数据的前几行和后几行以进行调试
    #print(trade_cal_df.head())
    #print(trade_cal_df.tail())

    # 获取前天的日期，格式为 'YYYY-MM-DD'
    day_before_yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 打印前天的日期以进行调试
    #print(f"Day before yesterday: {day_before_yesterday}")

    if is_market_open(day_before_yesterday, trade_cal_df):
        print(f"{day_before_yesterday} 是交易日")

        # 获取前天的本地日期格式
        current_date_local = (datetime.today() - timedelta(days=1)).strftime('%Y年%m月%d日')

        # 使用前天的日期进行查询
        query_string = f'{current_date_local}非停牌的股票'

        # 查询非停牌的股票，并按股票代码升序排列
        res = pywencai.get(query=query_string, sort_key='股票代码', loop=True, sort_order='asc')

        # 检查是否有结果
        if res.empty:
            print("No data found for the query.")
        else:
            # 保存数据到 MongoDB
            save_to_mongodb(res, day_before_yesterday)
    else:
        print(f"{day_before_yesterday} 不是交易日，程序退出")
