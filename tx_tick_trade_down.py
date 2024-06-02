import pandas as pd
import akshare as ak
import time
import os
import shutil
from datetime import datetime

# Specify the input file path
input_file_path = '/opt/akshare_tick/ashare1800_modified.csv'

# Ensure the file exists
if not os.path.exists(input_file_path):
    raise FileNotFoundError(f"Input file {input_file_path} not found.")

# Read the CSV file
df = pd.read_csv(input_file_path)

# Ensure the 'tx_code' column exists
if 'tx_code' not in df.columns:
    raise ValueError("The input file does not contain a 'tx_code' column.")

# Generate the output directory based on the current date
today = datetime.today()
output_dir = f'/mnt/vdb1/tick_data/{today.year}/{today.month:02}/{today.day:02}'
os.makedirs(output_dir, exist_ok=True)

# Iterate over the symbols in 'tx_code' column
for symbol in df['tx_code']:
    try:
        # Fetch the stock data using Akshare
        stock_data = ak.stock_zh_a_tick_tx_js(symbol=symbol)
        # Add the symbol column to the DataFrame
        stock_data['symbol'] = symbol
        # Save the data to a CSV file
        stock_data.to_csv(os.path.join(output_dir, f'{symbol}.csv'), index=False)
        print(f"Saved data for {symbol} to {output_dir}")
        # Delay to avoid hitting rate limits
        time.sleep(1)
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

# Compress the output directory
compressed_dir = f'/mnt/vdb1/tick_data_tar/{today.year}/{today.month:02}'
os.makedirs(compressed_dir, exist_ok=True)
compressed_path = os.path.join(compressed_dir, f'{today.year}-{today.month:02}-{today.day:02}.tar.gz')
shutil.make_archive(compressed_path.replace('.tar.gz', ''), 'gztar', output_dir)
print(f"Compressed data to {compressed_path}")
