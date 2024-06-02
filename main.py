import akshare as ak
from datetime import datetime
import pywencai
import pandas as pd
import time
import os
import shutil

def clean_stock_code(stock_code):
    """
    清洗股票代码
    :param stock_code: 股票代码，格式为 'XXXXXX.SZ'
    :return: 清洗后的股票代码，格式为 'szXXXXXX'
    """
    return stock_code[7:].lower() + stock_code[:6]

if __name__ == "__main__":
    # 获取当前日期
    today = datetime.today().strftime('%Y%m%d')

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
        # 清洗股票代码
        res['股票代码'] = res['股票代码'].apply(clean_stock_code)

        # 生成基于当前日期的输出目录
        output_dir = f'/mnt/vdb1/tick_data/{today.year}/{today.month:02}/{today.day:02}'
        os.makedirs(output_dir, exist_ok=True)

        # 遍历股票代码列
        for symbol in res['股票代码']:
            try:
                # 使用Akshare获取股票数据
                stock_data = ak.stock_zh_a_tick_tx_js(symbol=symbol)
                # 向DataFrame中添加股票代码列
                stock_data['symbol'] = symbol
                # 保存数据到CSV文件
                stock_data.to_csv(os.path.join(output_dir, f'{symbol}.csv'), index=False)
                print(f"Saved data for {symbol} to {output_dir}")
                # 延迟以避免触及频率限制
                time.sleep(1)
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")

        # 压缩输出目录
        compressed_dir = f'/mnt/vdb1/tick_data_tar/{today.year}/{today.month:02}'
        os.makedirs(compressed_dir, exist_ok=True)
        compressed_path = os.path.join(compressed_dir, f'{today.year}-{today.month:02}-{today.day:02}.tar.gz')
        shutil.make_archive(compressed_path.replace('.tar.gz', ''), 'gztar', output_dir)
        print(f"Compressed data to {compressed_path}")
