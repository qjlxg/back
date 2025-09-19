import pandas as pd
import requests
import re
import os
import logging
import tenacity
from datetime import datetime, time
from io import StringIO
import random
import time as time_module

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 定义本地数据存储目录
DATA_DIR = 'index_data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# 配置指数代码和文件名
# 沪深300指数在天天基金网上的代码是 '000300'
INDEX_CODE = '000300'
OUTPUT_FILE = os.path.join(DATA_DIR, f'{INDEX_CODE}.csv')

# 网络请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
}

@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_fixed(10),
    retry=tenacity.retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
    before_sleep=lambda retry_state: logger.info(f"下载大盘指数失败，正在重试... 第 {retry_state.attempt_number} 次")
)
def fetch_and_save_index_data():
    """
    从天天基金网下载沪深300指数历史净值数据并保存为CSV文件。
    """
    logger.info("开始下载大盘指数历史净值数据 (%s)...", INDEX_CODE)
    
    all_index_data = []
    page_index = 1
    
    while True:
        url = f"http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={INDEX_CODE}&page={page_index}&per=20"
        logger.info("正在获取大盘指数 %s 的第 %d 页数据...", INDEX_CODE, page_index)
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            content_match = re.search(r'content:"(.*?)"', response.text, re.S)
            pages_match = re.search(r'pages:(\d+)', response.text)
            
            if not content_match or not pages_match:
                logger.error("API返回内容格式不正确，可能已无数据或接口变更。")
                break

            raw_content_html = content_match.group(1).replace('\\"', '"')
            total_pages = int(pages_match.group(1))
            
            tables = pd.read_html(StringIO(raw_content_html))
            
            if not tables:
                logger.warning("在第 %d 页未找到数据表格，爬取结束。", page_index)
                break
            
            df_page = tables[0]
            # 修正列名，只保留日期和单位净值（即净值）
            df_page.columns = ['date', 'net_value', 'cumulative_net_value', 'daily_growth_rate', 'purchase_status', 'redemption_status', 'dividend']
            df_page = df_page[['date', 'net_value']].copy()
            df_page['date'] = pd.to_datetime(df_page['date'], errors='coerce')
            df_page['net_value'] = pd.to_numeric(df_page['net_value'], errors='coerce')
            df_page = df_page.dropna(subset=['date', 'net_value'])
            
            if df_page.empty:
                logger.info("第 %d 页无有效数据，爬取结束。", page_index)
                break
            
            all_index_data.append(df_page)
            
            if page_index >= total_pages:
                logger.info("已获取所有历史数据，共 %d 页，爬取结束。", total_pages)
                break
            
            page_index += 1
            time_module.sleep(random.uniform(1, 2))
            
        except requests.exceptions.RequestException as e:
            logger.error("API请求失败: %s", str(e))
            raise
        except Exception as e:
            logger.error("数据解析失败: %s", str(e))
            raise

    if all_index_data:
        combined_df = pd.concat(all_index_data, ignore_index=True)
        # 去重并按日期排序
        combined_df = combined_df.drop_duplicates(subset=['date']).sort_values(by='date', ascending=True)
        
        # 保存到本地CSV文件
        combined_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        logger.info("成功下载并保存数据到: %s", OUTPUT_FILE)
        logger.info("最新数据日期: %s", combined_df['date'].max().date())
    else:
        logger.warning("未获取到任何指数数据。")

if __name__ == '__main__':
    fetch_and_save_index_data()
