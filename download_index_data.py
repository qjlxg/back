import pandas as pd
import akshare as ak
import os
from datetime import datetime
import logging

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
INDEX_CODE = '000001' # 默认下载上证指数
OUTPUT_FILE = os.path.join(DATA_DIR, f'{INDEX_CODE}.csv')

def fetch_and_save_index_data():
    """使用AkShare下载上证指数历史数据并保存为CSV文件"""
    logger.info("开始下载大盘指数历史数据 (%s)", INDEX_CODE)
    try:
        # 使用akshare获取指定指数历史数据，日线级别
        index_data_df = ak.stock_zh_index_daily(symbol=INDEX_CODE)
        
        # 转换日期格式并设置索引
        index_data_df['date'] = pd.to_datetime(index_data_df['date']).dt.date
        index_data_df.set_index('date', inplace=True)
        
        # 保存到本地CSV文件
        index_data_df.to_csv(OUTPUT_FILE, encoding='utf-8')
        
        logger.info("成功下载并保存数据到: %s", OUTPUT_FILE)
        logger.info("最新数据日期: %s", index_data_df.index.max())
        
    except Exception as e:
        logger.error("下载大盘数据失败: %s", e)
        logger.warning("本次运行将使用上次成功下载的本地数据。")

if __name__ == '__main__':
    fetch_and_save_index_data()
