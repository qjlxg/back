import pandas as pd
import yfinance as yf
import os
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 定义本地数据存储目录和文件
DATA_DIR = 'index_data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 配置指数代码和文件名
# 对于沪深300指数，Yahoo Finance 的代码是 '000300.SS'
INDEX_CODE = '000300.SS'
OUTPUT_FILE = os.path.join(DATA_DIR, '000300.csv') # 保存为000300.csv

def fetch_and_save_index_data():
    """使用 yfinance 下载沪深300指数历史数据并保存为CSV文件"""
    logger.info("开始下载大盘指数历史数据 (%s)", INDEX_CODE)
    try:
        # 使用 yfinance 获取沪深300指数的历史数据
        index_data_df = yf.download(INDEX_CODE, period="max", interval="1d")
        
        # 将日期索引转换为日期列
        index_data_df.reset_index(inplace=True)

        # 修正列名以与 market_monitor.py 兼容
        index_data_df.rename(columns={
            'Date': 'date', # 将 'Date' 改为 'date'
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close', # 将 'Close' 改为 'close'
            'Volume': 'volume'
        }, inplace=True)
        
        # 转换日期格式，并确保只保留需要的列
        index_data_df['date'] = pd.to_datetime(index_data_df['date']).dt.date
        index_data_df = index_data_df[['date', 'open', 'high', 'low', 'close', 'volume']]
        
        # 保存到本地CSV文件
        index_data_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        logger.info("成功下载并保存数据到: %s", OUTPUT_FILE)
        logger.info("最新数据日期: %s", index_data_df['date'].max())
        
    except Exception as e:
        logger.error("下载大盘数据失败: %s", e)
        logger.warning("本次运行将使用上次成功下载的本地数据。")

if __name__ == '__main__':
    fetch_and_save_index_data()
