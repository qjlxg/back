
import pandas as pd
import numpy as np
import re
import os
import logging
from datetime import datetime, timedelta, time
import random
from io import StringIO
import requests
import tenacity
import concurrent.futures
import time as time_module

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('market_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 定义本地数据存储目录
DATA_DIR = 'fund_data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md', backtest_output_file='backtest_report.md'):
        self.report_file = report_file
        self.output_file = output_file
        self.backtest_output_file = backtest_output_file
        self.fund_codes = []
        self.fund_data = {}
        self.index_code = '000300'  # 沪深300指数代码
        self.index_data = {}
        self.index_df = pd.DataFrame()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

    def _get_expected_latest_date(self):
        """根据当前时间确定期望的最新数据日期"""
        now = datetime.now()
        # 假设净值更新时间为晚上21:00
        update_time = time(21, 0)
        if now.time() < update_time:
            # 如果当前时间早于21:00，则期望最新日期为昨天
            expected_date = now.date() - timedelta(days=1)
        else:
            # 否则，期望最新日期为今天
            expected_date = now.date()
        logger.info("当前时间: %s, 期望最新数据日期: %s", now.strftime('%Y-%m-%d %H:%M:%S'), expected_date)
        return expected_date

    def _parse_report(self, report_path='analysis_report.md'):
        """从 analysis_report.md 提取推荐基金代码"""
        logger.info("正在解析 %s 获取推荐基金代码...", report_path)
        if not os.path.exists(report_path):
            logger.error("报告文件 %s 不存在", report_path)
            raise FileNotFoundError(f"{report_path} 不存在")
        
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            pattern = re.compile(r'(?:^\| +(\d{6})|### 基金 (\d{6}))', re.M)
            matches = pattern.findall(content)

            extracted_codes = set()
            for match in matches:
                code = match[0] if match[0] else match[1]
                extracted_codes.add(code)
            
            sorted_codes = sorted(list(extracted_codes))
            self.fund_codes = sorted_codes[:10]
            
            if not self.fund_codes:
                logger.warning("未提取到任何有效基金代码，请检查 analysis_report.md")
            else:
                logger.info("提取到 %d 个基金（测试限制前10个）: %s", len(self.fund_codes), self.fund_codes)
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            raise

    def _read_local_data(self, fund_code):
        """读取本地文件，如果存在则返回DataFrame"""
        file_path = os.path.join(DATA_DIR, f"{fund_code}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, parse_dates=['date'])
                if not df.empty and 'date' in df.columns and 'net_value' in df.columns:
                    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
                    logger.info("本地已存在基金 %s 数据，共 %d 行，最新日期为: %s", fund_code, len(df), df['date'].max().date())
                    return df
            except Exception as e:
                logger.warning("读取本地文件 %s 失败: %s", file_path, e)
        return pd.DataFrame()

    def _save_to_local_file(self, fund_code, df):
        """将DataFrame保存到本地文件，覆盖旧文件"""
        file_path = os.path.join(DATA_DIR, f"{fund_code}.csv")
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        df.to_csv(file_path, index=False)
        logger.info("基金 %s 数据已成功保存到本地文件: %s", fund_code, file_path)

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(10),
        retry=tenacity.retry_if_exception_type((requests.exceptions.RequestException, ValueError)),
        before_sleep=lambda retry_state: logger.info(f"重试基金 {retry_state.args[0]}，第 {retry_state.attempt_number} 次")
    )
    def _fetch_fund_data(self, fund_code, latest_local_date=None):
        """
        从网络获取基金数据，实现真正的增量更新。
        如果 latest_local_date 不为空，则只获取其之后的数据。
        """
        all_new_data = []
        page_index = 1
        has_new_data = False
        
        while True:
            url = f"http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={fund_code}&page={page_index}&per=20"
            logger.info("正在获取基金 %s 的第 %d 页数据...", fund_code, page_index)
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                content_match = re.search(r'content:"(.*?)"', response.text, re.S)
                pages_match = re.search(r'pages:(\d+)', response.text)
                
                if not content_match or not pages_match:
                    logger.error("基金 %s API返回内容格式不正确，可能已无数据或接口变更", fund_code)
                    break

                raw_content_html = content_match.group(1).replace('\\"', '"')
                total_pages = int(pages_match.group(1))
                
                tables = pd.read_html(StringIO(raw_content_html))
                
                if not tables:
                    logger.warning("基金 %s 在第 %d 页未找到数据表格，爬取结束", fund_code, page_index)
                    break
                
                df_page = tables[0]
                df_page.columns = ['date', 'net_value', 'cumulative_net_value', 'daily_growth_rate', 'purchase_status', 'redemption_status', 'dividend']
                df_page = df_page[['date', 'net_value']].copy()
                df_page['date'] = pd.to_datetime(df_page['date'], errors='coerce')
                df_page['net_value'] = pd.to_numeric(df_page['net_value'], errors='coerce')
                df_page = df_page.dropna(subset=['date', 'net_value'])
                
                # 如果是增量更新模式，检查是否已获取到本地最新数据之前的数据
                if latest_local_date:
                    new_df_page = df_page[df_page['date'].dt.date > latest_local_date]
                    if new_df_page.empty:
                        # 如果当前页没有新数据，且之前已经发现过新数据，则停止爬取
                        if has_new_data:
                            logger.info("基金 %s 已获取所有新数据，爬取结束。", fund_code)
                            break
                        # 如果当前页没有新数据，且是第一页，则说明没有新数据
                        elif page_index == 1:
                            logger.info("基金 %s 无新数据，爬取结束。", fund_code)
                            break
                    else:
                        has_new_data = True
                        all_new_data.append(new_df_page)
                        logger.info("第 %d 页: 发现 %d 行新数据", page_index, len(new_df_page))
                else:
                    # 如果是首次下载，则获取所有数据
                    all_new_data.append(df_page)

                logger.info("基金 %s 总页数: %d, 当前页: %d, 当前页行数: %d", fund_code, total_pages, page_index, len(df_page))
                
                # 如果是增量更新模式，且当前页数据比最新数据日期早，则结束循环
                if latest_local_date and (df_page['date'].dt.date <= latest_local_date).any():
                    logger.info("基金 %s 已追溯到本地数据，增量爬取结束。", fund_code)
                    break

                if page_index >= total_pages:
                    logger.info("基金 %s 已获取所有历史数据，共 %d 页，爬取结束", fund_code, total_pages)
                    break
                
                page_index += 1
                time_module.sleep(random.uniform(1, 2))  # 延长sleep到1-2秒，减少限速风险
                
            except requests.exceptions.RequestException as e:
                logger.error("基金 %s API请求失败: %s", fund_code, str(e))
                raise
            except Exception as e:
                logger.error("基金 %s API数据解析失败: %s", fund_code, str(e))
                raise

        # 合并新数据并返回
        if all_new_data:
            new_combined_df = pd.concat(all_new_data, ignore_index=True)
            return new_combined_df[['date', 'net_value']]
        else:
            return pd.DataFrame()

    def _fetch_index_data(self, latest_local_date=None):
        """
        从网络获取大盘指数数据（沪深300），实现增量更新。
        """
        all_new_data = []
        page_index = 1
        has_new_data = False
        
        while True:
            url = f"http://fundf10.eastmoney.com/F10DataApi.aspx?type=lsjz&code={self.index_code}&page={page_index}&per=20"
            logger.info("正在获取大盘指数 %s 的第 %d 页数据...", self.index_code, page_index)
            
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                content_match = re.search(r'content:"(.*?)"', response.text, re.S)
                pages_match = re.search(r'pages:(\d+)', response.text)
                
                if not content_match or not pages_match:
                    logger.error("大盘指数 %s API返回内容格式不正确，可能已无数据或接口变更", self.index_code)
                    break

                raw_content_html = content_match.group(1).replace('\\"', '"')
                total_pages = int(pages_match.group(1))
                
                tables = pd.read_html(StringIO(raw_content_html))
                
                if not tables:
                    logger.warning("大盘指数 %s 在第 %d 页未找到数据表格，爬取结束", self.index_code, page_index)
                    break
                
                df_page = tables[0]
                df_page.columns = ['date', 'net_value', 'cumulative_net_value', 'daily_growth_rate', 'purchase_status', 'redemption_status', 'dividend']
                df_page = df_page[['date', 'net_value']].copy()
                df_page['date'] = pd.to_datetime(df_page['date'], errors='coerce')
                df_page['net_value'] = pd.to_numeric(df_page['net_value'], errors='coerce')
                df_page = df_page.dropna(subset=['date', 'net_value'])
                
                # 如果是增量更新模式，检查是否已获取到本地最新数据之前的数据
                if latest_local_date:
                    new_df_page = df_page[df_page['date'].dt.date > latest_local_date]
                    if new_df_page.empty:
                        # 如果当前页没有新数据，且之前已经发现过新数据，则停止爬取
                        if has_new_data:
                            logger.info("大盘指数 %s 已获取所有新数据，爬取结束。", self.index_code)
                            break
                        # 如果当前页没有新数据，且是第一页，则说明没有新数据
                        elif page_index == 1:
                            logger.info("大盘指数 %s 无新数据，爬取结束。", self.index_code)
                            break
                    else:
                        has_new_data = True
                        all_new_data.append(new_df_page)
                        logger.info("第 %d 页: 发现 %d 行新数据", page_index, len(new_df_page))
                else:
                    # 如果是首次下载，则获取所有数据
                    all_new_data.append(df_page)

                logger.info("大盘指数 %s 总页数: %d, 当前页: %d, 当前页行数: %d", self.index_code, total_pages, page_index, len(df_page))
                
                # 如果是增量更新模式，且当前页数据比最新数据日期早，则结束循环
                if latest_local_date and (df_page['date'].dt.date <= latest_local_date).any():
                    logger.info("大盘指数 %s 已追溯到本地数据，增量爬取结束。", self.index_code)
                    break

                if page_index >= total_pages:
                    logger.info("大盘指数 %s 已获取所有历史数据，共 %d 页，爬取结束", self.index_code, total_pages)
                    break
                
                page_index += 1
                time_module.sleep(random.uniform(1, 2))  # 延长sleep到1-2秒，减少限速风险
                
            except requests.exceptions.RequestException as e:
                logger.error("大盘指数 %s API请求失败: %s", self.index_code, str(e))
                raise
            except Exception as e:
                logger.error("大盘指数 %s API数据解析失败: %s", self.index_code, str(e))
                raise

        # 合并新数据并返回
        if all_new_data:
            new_combined_df = pd.concat(all_new_data, ignore_index=True)
            return new_combined_df[['date', 'net_value']]
        else:
            return pd.DataFrame()

    def _calculate_indicators(self, df):
        """计算技术指标并生成结果字典"""
        if df is None or df.empty or len(df) < 26:
            return None

        df = df.sort_values(by='date', ascending=True)
        
        # MACD
        exp12 = df['net_value'].ewm(span=12, adjust=False).mean()
        exp26 = df['net_value'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()

        # 布林带
        window = 20
        df['bb_mid'] = df['net_value'].rolling(window=window, min_periods=1).mean()
        df['bb_std'] = df['net_value'].rolling(window=window, min_periods=1).std()
        df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * 2)
        df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * 2)
        
        # RSI
        delta = df['net_value'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=14, min_periods=1).mean()
        avg_loss = loss.rolling(window=14, min_periods=1).mean()
        
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi'] = 100 - (100 / (1 + rs))

        # MA50
        df['ma50'] = df['net_value'].rolling(window=min(50, len(df)), min_periods=1).mean()
        df['ma_ratio'] = df['net_value'] / df['ma50']

        return df

    def _get_latest_signals(self, fund_code, df):
        """根据最新数据计算信号"""
        try:
            processed_df = self._calculate_indicators(df)
            if processed_df is None:
                logger.warning("基金 %s 数据不足，跳过计算", fund_code)
                return {
                    'fund_code': fund_code, 'latest_net_value': "数据获取失败", 'rsi': np.nan, 'ma_ratio': np.nan,
                    'macd_diff': np.nan, 'bb_upper': np.nan, 'bb_lower': np.nan, 'bb_position': 'N/A', 'advice': "观察", 'action_signal': 'N/A'
                }
            
            latest_data = processed_df.iloc[-1]
            latest_net_value = latest_data['net_value']
            latest_rsi = latest_data['rsi']
            latest_ma50_ratio = latest_data['ma_ratio']
            latest_macd_diff = latest_data['macd'] - latest_data['signal']
            latest_bb_upper = latest_data['bb_upper']
            latest_bb_lower = latest_data['bb_lower']

            advice = "观察"
            if (not np.isnan(latest_rsi) and latest_rsi > 70) or \
               (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
               (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2):
                advice = "等待回调"
            elif (not np.isnan(latest_rsi) and latest_rsi < 30) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.8):
                advice = "可分批买入"
            elif (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                advice = "可分批买入"
            elif (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 1) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                advice = "等待回调"

            action_signal = "持有/观察"
            if not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.95:
                action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 70) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 65) or \
                 (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > 1.2):
                action_signal = "弱卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi < 35) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 0.9) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                action_signal = "强买入"
            elif (not np.isnan(latest_rsi) and latest_rsi < 45) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 1):
                action_signal = "弱买入"

            # 计算布林带位置
            bb_position = "中轨"
            if not np.isnan(latest_net_value) and not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper:
                bb_position = "上轨上方"
            elif not np.isnan(latest_net_value) and not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower:
                bb_position = "下轨下方"

            return {
                'fund_code': fund_code,
                'latest_net_value': latest_net_value,
                'rsi': latest_rsi,
                'ma_ratio': latest_ma50_ratio,
                'macd_diff': latest_macd_diff,
                'bb_upper': latest_bb_upper,
                'bb_lower': latest_bb_lower,
                'bb_position': bb_position,
                'advice': advice,
                'action_signal': action_signal
            }
        except Exception as e:
            logger.error("处理基金 %s 时发生异常: %s", fund_code, str(e))
            return {
                'fund_code': fund_code,
                'latest_net_value': "数据获取失败",
                'rsi': np.nan,
                'ma_ratio': np.nan,
                'macd_diff': np.nan,
                'bb_upper': np.nan,
                'bb_lower': np.nan,
                'bb_position': 'N/A',
                'advice': "观察",
                'action_signal': 'N/A'
            }

    def _analyze_index(self):
        """分析大盘指数，计算技术指标和信号"""
        logger.info("开始分析大盘指数 %s...", self.index_code)
        local_df = self._read_local_data(self.index_code)
        latest_local_date = local_df['date'].max().date() if not local_df.empty else None

        new_df = self._fetch_index_data(latest_local_date)
        
        if not new_df.empty:
            df_final = pd.concat([local_df, new_df]).drop_duplicates(subset=['date'], keep='last').sort_values(by='date', ascending=True)
            self._save_to_local_file(self.index_code, df_final)
            self.index_df = df_final
            result = self._get_latest_signals(self.index_code, df_final.tail(100))
            self.index_data = result
            logger.info("大盘指数 %s 分析完成", self.index_code)
        elif not local_df.empty:
            # 如果没有新数据，且本地有数据，则使用本地数据计算信号
            logger.info("大盘指数 %s 无新数据，使用本地历史数据进行分析", self.index_code)
            self.index_df = local_df
            result = self._get_latest_signals(self.index_code, local_df.tail(100))
            self.index_data = result
            logger.info("大盘指数 %s 分析完成", self.index_code)
        else:
            # 如果既没有新数据，本地又没有数据，则返回失败
            logger.error("大盘指数 %s 未获取到任何有效数据，且本地无缓存", self.index_code)
            self.index_data = {
                'fund_code': self.index_code,
                'latest_net_value': "数据获取失败",
                'rsi': np.nan,
                'ma_ratio': np.nan,
                'macd_diff': np.nan,
                'bb_upper': np.nan,
                'bb_lower': np.nan,
                'bb_position': 'N/A',
                'advice': "观察",
                'action_signal': 'N/A'
            }

    def get_fund_data(self):
        """主控函数：优先从本地加载，仅在数据非最新或不完整时下载"""
        # 步骤1: 解析推荐基金代码
        self._parse_report()
        if not self.fund_codes:
            logger.error("没有提取到任何基金代码，无法继续处理")
            return

        # 步骤2: 预加载本地数据并检查是否需要下载
        logger.info("开始预加载本地缓存数据...")
        fund_codes_to_fetch = []
        expected_latest_date = self._get_expected_latest_date()
        min_data_points = 26  # 确保有足够数据计算技术指标

        for fund_code in self.fund_codes:
            local_df = self._read_local_data(fund_code)
            
            if not local_df.empty:
                latest_local_date = local_df['date'].max().date()
                data_points = len(local_df)
                
                # 检查数据是否最新且完整
                if latest_local_date >= expected_latest_date and data_points >= min_data_points:
                    logger.info("基金 %s 的本地数据已是最新 (%s, 期望: %s) 且数据量足够 (%d 行)，直接加载。",
                                 fund_code, latest_local_date, expected_latest_date, data_points)
                    self.fund_data[fund_code] = self._get_latest_signals(fund_code, local_df.tail(100))
                    continue
                else:
                    if latest_local_date < expected_latest_date:
                        logger.info("基金 %s 本地数据已过时（最新日期为 %s，期望 %s），需要从网络获取新数据。",
                                     fund_code, latest_local_date, expected_latest_date)
                    if data_points < min_data_points:
                        logger.info("基金 %s 本地数据量不足（仅 %d 行，需至少 %d 行），需要从网络获取。",
                                     fund_code, data_points, min_data_points)
            else:
                logger.info("基金 %s 本地数据不存在，需要从网络获取。", fund_code)
            
            fund_codes_to_fetch.append(fund_code)

        # 步骤3: 多线程网络下载和处理
        if fund_codes_to_fetch:
            logger.info("开始使用多线程获取 %d 个基金的新数据...", len(fund_codes_to_fetch))
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_code = {executor.submit(self._process_single_fund, code): code for code in fund_codes_to_fetch}
                for future in concurrent.futures.as_completed(future_to_code):
                    fund_code = future_to_code[future]
                    try:
                        result = future.result()
                        if result:
                            self.fund_data[fund_code] = result
                    except Exception as e:
                        logger.error("处理基金 %s 数据时出错: %s", fund_code, str(e))
                        self.fund_data[fund_code] = {
                            'fund_code': fund_code, 'latest_net_value': "数据获取失败", 'rsi': np.nan,
                            'ma_ratio': np.nan, 'macd_diff': np.nan, 'bb_upper': np.nan, 'bb_lower': np.nan, 
                            'bb_position': 'N/A', 'advice': "观察", 'action_signal': 'N/A'
                        }
        else:
            logger.info("所有基金数据均来自本地缓存，无需网络下载。")
        
        if len(self.fund_data) > 0:
            logger.info("所有基金数据处理完成。")
        else:
            logger.error("所有基金数据均获取失败。")

        # 新增：分析大盘指数
        self._analyze_index()

    def _process_single_fund(self, fund_code):
        """处理单个基金数据：读取本地，下载增量，合并，保存，并计算信号"""
        local_df = self._read_local_data(fund_code)
        latest_local_date = local_df['date'].max().date() if not local_df.empty else None

        new_df = self._fetch_fund_data(fund_code, latest_local_date)
        
        if not new_df.empty:
            df_final = pd.concat([local_df, new_df]).drop_duplicates(subset=['date'], keep='last').sort_values(by='date', ascending=True)
            self._save_to_local_file(fund_code, df_final)
            return self._get_latest_signals(fund_code, df_final.tail(100))
        elif not local_df.empty:
            # 如果没有新数据，且本地有数据，则使用本地数据计算信号
            logger.info("基金 %s 无新数据，使用本地历史数据进行分析", fund_code)
            return self._get_latest_signals(fund_code, local_df.tail(100))
        else:
            # 如果既没有新数据，本地又没有数据，则返回失败
            logger.error("基金 %s 未获取到任何有效数据，且本地无缓存", fund_code)
            return None
    
    def _backtest_strategy(self, fund_code, df):
        """历史回测策略性能"""
        if df is None or df.empty or len(df) < 100:
            logger.warning("基金 %s 数据不足，无法回测", fund_code)
            return {"cum_return": np.nan, "max_drawdown": np.nan, "sharpe_ratio": np.nan, "win_rate": np.nan, "cagr": np.nan, "total_trades": 0}

        # 计算所有指标
        df = self._calculate_indicators(df)
        df = df.dropna()
        # 关键修复：重置索引以确保后续循环的.iloc正常工作
        df.reset_index(drop=True, inplace=True)

        # 增加一个检查，确保dropna后仍有足够的数据
        if df.empty or len(df) < 2:
            logger.warning("基金 %s 计算指标后数据不足，无法回测", fund_code)
            return {"cum_return": np.nan, "max_drawdown": np.nan, "sharpe_ratio": np.nan, "win_rate": np.nan, "cagr": np.nan, "total_trades": 0}

        # 模拟交易
        position = 0
        buy_price = 0
        trades = []
        equity = [1.0] * len(df)
        
        for i in range(1, len(df)):
            latest_data = df.iloc[i]
            latest_net_value = latest_data['net_value']
            
            # 最大回撤计算
            prev_net_value = df['net_value'].iloc[i-1]
            if prev_net_value != 0:
                equity[i] = equity[i-1] * (1 + (latest_net_value - prev_net_value) / prev_net_value)
            else:
                equity[i] = equity[i-1]

            # 止损逻辑
            if position == 1 and (latest_net_value / buy_price) < 0.90:  # 止损10%
                sell_price = latest_net_value
                ret = (sell_price - buy_price) / buy_price
                trades.append({'buy_date': df.iloc[i-1]['date'], 'sell_date': df.iloc[i]['date'], 'return': ret, 'type': 'stop_loss'})
                position = 0
                buy_price = 0
                continue # 继续下一天

            # 交易信号逻辑
            latest_rsi = latest_data['rsi']
            latest_ma_ratio = latest_data['ma_ratio']
            latest_macd_diff = latest_data['macd'] - latest_data['signal']
            latest_bb_upper = latest_data['bb_upper']
            latest_bb_lower = latest_data['bb_lower']

            action_signal = "持有/观察"
            if not np.isnan(latest_ma_ratio) and latest_ma_ratio < 0.95:
                action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 70) and \
                 (not np.isnan(latest_ma_ratio) and latest_ma_ratio > 1.2) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > 65) or \
                 (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
                 (not np.isnan(latest_ma_ratio) and latest_ma_ratio > 1.2):
                action_signal = "弱卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi < 35) and \
                 (not np.isnan(latest_ma_ratio) and latest_ma_ratio < 0.9) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                action_signal = "强买入"
            elif (not np.isnan(latest_rsi) and latest_rsi < 45) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma_ratio) and latest_ma_ratio < 1):
                action_signal = "弱买入"
            
            # 模拟交易
            if action_signal in ["强买入", "弱买入"] and position == 0:
                position = 1
                buy_price = latest_net_value
                # 修复: 使用 .iloc 进行基于位置的索引
                trades.append({'buy_date': df.iloc[i]['date'], 'buy_price': buy_price})
            elif action_signal in ["强卖出/规避", "弱卖出/规避"] and position == 1:
                sell_price = latest_net_value
                ret = (sell_price - buy_price) / buy_price
                trades[-1]['sell_date'] = df.iloc[i]['date']
                trades[-1]['sell_price'] = sell_price
                trades[-1]['return'] = ret
                position = 0

        # 如果最后还持有，则以最后一天净值卖出
        if position == 1:
            sell_price = df.iloc[-1]['net_value']
            ret = (sell_price - buy_price) / buy_price
            trades[-1]['sell_date'] = df.iloc[-1]['date']
            trades[-1]['sell_price'] = sell_price
            trades[-1]['return'] = ret

        # 计算回测指标
        if trades:
            returns = [trade['return'] for trade in trades if 'return' in trade]
            cum_return = np.prod([1 + r for r in returns]) - 1 if returns else 0
            win_rate = len([r for r in returns if r > 0]) / len(returns) if returns else 0
            total_trades = len(trades)
        else:
            cum_return = 0
            win_rate = 0
            total_trades = 0
        
        # 计算年化收益率
        start_date = df['date'].iloc[0]
        end_date = df['date'].iloc[-1]
        years = (end_date - start_date).days / 365.25
        cagr = (1 + cum_return) ** (1/years) - 1 if years > 0 else 0

        # 计算最大回撤
        equity_series = pd.Series(equity)
        roll_max = equity_series.cummax()
        drawdown = equity_series / roll_max - 1
        max_drawdown = drawdown.min()

        # 计算夏普比率
        daily_returns = pd.Series(equity).pct_change().dropna()
        sharpe_ratio = np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252) if len(daily_returns) > 1 and np.std(daily_returns) > 0 else np.nan

        logger.info("基金 %s 回测结果: 累计回报=%.2f, 最大回撤=%.2f, 夏普比率=%.2f, 胜率=%.2f, 年化收益率=%.2f, 交易次数=%d", 
                      fund_code, cum_return, max_drawdown, sharpe_ratio if not np.isnan(sharpe_ratio) else -1, win_rate, cagr, total_trades)
        
        return {
            "cum_return": cum_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "cagr": cagr,
            "total_trades": total_trades
        }

    def generate_report(self):
        """生成市场情绪与技术指标监控报告"""
        logger.info("正在生成市场监控报告...")
        report_df_list = []
        for fund_code in self.fund_codes:
            data = self.fund_data.get(fund_code)
            if data is not None:
                latest_net_value_str = f"{data['latest_net_value']:.4f}" if isinstance(data['latest_net_value'], (float, int)) else str(data['latest_net_value'])
                rsi_str = f"{data['rsi']:.2f}" if isinstance(data['rsi'], (float, int)) and not np.isnan(data['rsi']) else "N/A"
                ma_ratio_str = f"{data['ma_ratio']:.2f}" if isinstance(data['ma_ratio'], (float, int)) and not np.isnan(data['ma_ratio']) else "N/A"
                
                macd_signal = "N/A"
                if isinstance(data['macd_diff'], (float, int)) and not np.isnan(data['macd_diff']):
                    macd_signal = "金叉" if data['macd_diff'] > 0 else "死叉"
                
                # 使用计算好的bb_position
                bollinger_pos = data.get('bb_position', "中轨")
                
                report_df_list.append({
                    "基金代码": fund_code,
                    "最新净值": latest_net_value_str,
                    "RSI": rsi_str,
                    "净值/MA50": ma_ratio_str,
                    "MACD信号": macd_signal,
                    "布林带位置": bollinger_pos,
                    "投资建议": data['advice'],
                    "行动信号": data['action_signal']
                })
            else:
                report_df_list.append({
                    "基金代码": fund_code,
                    "最新净值": "数据获取失败",
                    "RSI": "N/A",
                    "净值/MA50": "N/A",
                    "MACD信号": "N/A",
                    "布林带位置": "N/A",
                    "投资建议": "观察",
                    "行动信号": "N/A"
                })

        report_df = pd.DataFrame(report_df_list)

        # 定义排序优先级
        order_map_action = {
            "强买入": 1,
            "弱买入": 2,
            "持有/观察": 3,
            "弱卖出/规避": 4,
            "强卖出/规避": 5,
            "N/A": 6
        }
        order_map_advice = {
            "可分批买入": 1,
            "观察": 2,
            "等待回调": 3,
            "N/A": 4
        }
        
        report_df['sort_order_action'] = report_df['行动信号'].map(order_map_action)
        report_df['sort_order_advice'] = report_df['投资建议'].map(order_map_advice)
        
        # 将 NaN 替换为 N/A 并对净值等数据类型进行处理
        report_df['最新净值'] = pd.to_numeric(report_df['最新净值'], errors='coerce')
        report_df['RSI'] = pd.to_numeric(report_df['RSI'], errors='coerce')
        report_df['净值/MA50'] = pd.to_numeric(report_df['净值/MA50'], errors='coerce')

        # 按照您的新排序规则进行排序
        report_df = report_df.sort_values(
            by=['sort_order_action', 'sort_order_advice', 'RSI'],
            ascending=[True, True, True] # 优先按行动信号、其次按投资建议、最后按RSI从低到高排序
        ).drop(columns=['sort_order_action', 'sort_order_advice'])

        # 将浮点数格式化为字符串，方便Markdown输出
        report_df['最新净值'] = report_df['最新净值'].apply(lambda x: f"{x:.4f}" if not pd.isna(x) else "N/A")
        report_df['RSI'] = report_df['RSI'].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A")
        report_df['净值/MA50'] = report_df['净值/MA50'].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A")

        # 将上述排序后的 DataFrame 转换为 Markdown
        markdown_table = report_df.to_markdown(index=False)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场情绪与技术指标监控报告\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # 新增：大盘指数分析部分
            f.write(f"## 大盘指数分析 (沪深300)\n")
            f.write("此部分提供大盘整体市场情绪参考，用于辅助基金投资决策。\n\n")
            index_latest_net_value_str = f"{self.index_data['latest_net_value']:.2f}" if isinstance(self.index_data['latest_net_value'], (float, int)) else str(self.index_data['latest_net_value'])
            index_rsi_str = f"{self.index_data['rsi']:.2f}" if isinstance(self.index_data['rsi'], (float, int)) and not np.isnan(self.index_data['rsi']) else "N/A"
            index_ma_ratio_str = f"{self.index_data['ma_ratio']:.2f}" if isinstance(self.index_data['ma_ratio'], (float, int)) and not np.isnan(self.index_data['ma_ratio']) else "N/A"
            index_macd_signal = "N/A"
            if isinstance(self.index_data['macd_diff'], (float, int)) and not np.isnan(self.index_data['macd_diff']):
                index_macd_signal = "金叉" if self.index_data['macd_diff'] > 0 else "死叉"
            index_bollinger_pos = self.index_data.get('bb_position', "中轨")
            f.write("| 指数代码 | 最新点位 | RSI | 点位/MA50 | MACD信号 | 布林带位置 | 投资建议 | 行动信号 |\n")
            f.write("|----------|----------|-----|-----------|----------|------------|----------|----------|\n")
            f.write(f"| {self.index_code} | {index_latest_net_value_str} | {index_rsi_str} | {index_ma_ratio_str} | {index_macd_signal} | {index_bollinger_pos} | {self.index_data['advice']} | {self.index_data['action_signal']} |\n\n")

            f.write(f"## 推荐基金技术指标 (处理基金数: {len(self.fund_codes)})\n")
            f.write("此表格已按**行动信号优先级**排序，'强买入'基金将排在最前面。\n")
            f.write("**注意：** 当'行动信号'和'投资建议'冲突时，请以**行动信号**为准，其条件更严格，更适合机械化决策。\n")
            f.write("**大盘参考：** 请结合上方大盘分析结果，若大盘行动信号为'强卖出/规避'或'弱卖出/规避'，建议降低基金仓位。\n\n")
            f.write(markdown_table)
        
        logger.info("报告生成完成: %s", self.output_file)

    def _generate_backtest_report(self, backtest_results):
        """将回测结果输出为Markdown报告"""
        logger.info("正在生成回测报告...")
        if not backtest_results:
            logger.warning("回测结果为空，无法生成报告。")
            return

        report_df = pd.DataFrame.from_dict(backtest_results, orient='index')
        report_df = report_df.rename(columns={
            "cum_return": "累计回报",
            "max_drawdown": "最大回撤",
            "sharpe_ratio": "夏普比率",
            "win_rate": "胜率",
            "cagr": "年化收益率",
            "total_trades": "总交易次数"
        })

        # 格式化浮点数
        for col in ["累计回报", "最大回撤", "年化收益率", "胜率"]:
            report_df[col] = report_df[col].apply(lambda x: f"{x:.2%}" if not pd.isna(x) else "N/A")

        # 格式化夏普比率和总交易次数
        report_df['夏普比率'] = report_df['夏普比率'].apply(lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A")
        report_df['总交易次数'] = report_df['总交易次数'].astype(int)

        report_df = report_df.sort_values(by="累计回报", ascending=False)
        markdown_table = report_df.to_markdown()

        with open(self.backtest_output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 历史回测结果报告\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"此报告展示了将您的技术指标策略应用于历史数据的表现。\n\n")
            f.write("此表格已按**累计回报**从高到低排序。\n")
            f.write("注意：此回测加入了**10%止损**逻辑，以控制单笔交易亏损。\n\n")
            f.write(markdown_table)
        
        logger.info("回测报告生成完成: %s", self.backtest_output_file)

    def perform_backtest(self):
        """对所有基金进行历史回测，并输出结果"""
        backtest_results = {}
        for fund_code in self.fund_codes:
            df = self._read_local_data(fund_code)
            if not df.empty:
                backtest_results[fund_code] = self._backtest_strategy(fund_code, df)
            else:
                logger.warning("基金 %s 无历史数据，无法回测", fund_code)
                backtest_results[fund_code] = {"cum_return": np.nan, "max_drawdown": np.nan, "sharpe_ratio": np.nan, "win_rate": np.nan, "cagr": np.nan, "total_trades": 0}
        
        # 将结果保存到CSV文件
        backtest_df = pd.DataFrame.from_dict(backtest_results, orient='index')
        backtest_df.to_csv('backtest_results.csv', encoding='utf-8')
        logger.info("回测结果已保存到 backtest_results.csv")
        
        # 生成回测报告
        self._generate_backtest_report(backtest_results)

    def _get_portfolio_signals(self, fund_data, max_positions=5):
        """根据评分获取最佳买入机会，并限制最大持仓数"""
        buy_candidates = []
        for fund_code, data in fund_data.items():
            if data['action_signal'] in ["强买入", "弱买入"]:
                score = self._calculate_buy_score(data)
                buy_candidates.append({
                    'code': fund_code,
                    'signal': data['action_signal'],
                    'rsi': data['rsi'],
                    'ma_ratio': data['ma_ratio'],
                    'score': score
                })
        
        # 按分数从高到低排序，并限制数量
        buy_candidates.sort(key=lambda x: x['score'], reverse=True)
        return buy_candidates[:max_positions]

    def _calculate_buy_score(self, data):
        """根据多个指标计算买入评分，用于筛选"""
        score = 0
        
        # RSI评分
        if pd.isna(data['rsi']):
            score += 0
        elif data['rsi'] < 30:
            score += 40
        elif data['rsi'] < 45:
            score += 30
        else:
            score += 10

        # MA比率评分
        if pd.isna(data['ma_ratio']):
            score += 0
        elif data['ma_ratio'] < 0.9:
            score += 30
        elif data['ma_ratio'] < 1:
            score += 20
        else:
            score += 5

        # MACD评分
        if pd.isna(data['macd_diff']):
            score += 0
        elif data['macd_diff'] > 0:
            score += 10
        elif data['macd_diff'] < 0:
            score += 5
        else:
            score += 0

        # 布林带位置评分 - 直接使用已计算的 bb_position
        bb_position = data.get('bb_position', "中轨")
        if bb_position == "下轨下方":
            score += 25
        elif bb_position == "中轨":
            score += 15
        else:
            score += 5
        
        return score

    def generate_portfolio_recommendation(self):
        """生成投资组合推荐"""
        buy_candidates = self._get_portfolio_signals(self.fund_data, max_positions=3)
        
        print("\n" + "="*60)
        print("📊 今日投资组合推荐 (最多3支)")
        print("="*60)
        
        if buy_candidates:
            for i, candidate in enumerate(buy_candidates, 1):
                signal_emoji = "🟢" if candidate['signal'] == "强买入" else "🟡"
                print(f"{i}. {signal_emoji} {candidate['code']} "
                      f"(评分: {candidate['score']:.0f}, RSI: {candidate['rsi']:.1f})")
            if buy_candidates:
                suggested_amount = buy_candidates[0]['score'] // 10 * 100
                print(f"\n💰 建议分配: 每支{ suggested_amount }元")
                print(f"📈 今日买入机会: {len(buy_candidates)}/{len(self.fund_codes)}")
        else:
            print("❌ 今日无符合条件的买入机会，建议观望")
            print(f"📊 总扫描基金数: {len(self.fund_codes)}")


if __name__ == "__main__":
    try:
        logger.info("脚本启动")
        monitor = MarketMonitor()
        monitor.get_fund_data()
        monitor.generate_report()
        monitor.perform_backtest()
        monitor.generate_portfolio_recommendation()
        logger.info("脚本执行完成")
    except Exception as e:
        logger.error("脚本运行失败: %s", e, exc_info=True)
        raise
