import akshare as ak
from datetime import datetime
import pywencai

def is_market_open(date: str) -> bool:
    """
    查询指定日期是否为交易日

    :param date: 日期字符串，格式为 'YYYYMMDD'
    :return: 如果是交易日返回 True，否则返回 False
    """
    # 获取交易日历数据
    trade_cal_df = ak.tool_trade_date_hist_sina()

    # 判断指定日期是否在交易日历中
    if date in trade_cal_df['trade_date'].values:
        return True
    else:
        return False

if __name__ == "__main__":
    # 获取当前日期
    today = datetime.today().strftime('%Y%m%d')

    if is_market_open(today):
        print(f"{today} 是交易日")

        # 获取本地当前日期
        current_date = datetime.now().strftime('%Y年%m月%d日')

        # 使用当前日期进行查询
        query_string = f'{current_date}非停牌的股票'

        # 查询非停牌的股票，并按股票代码升序排列
        res = pywencai.get(query=query_string, sort_key='股票代码', loop=True, sort_order='asc')

        # 检查是否有结果
        if res.empty:
            print("No data found for the query.")
        else:
            # 将结果保存到本地CSV文件
            output_file = f'{today}_non_suspended_stocks.csv'
            res.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"The data has been saved to {output_file}")
    else:
        print(f"{today} 不是交易日，程序退出")
