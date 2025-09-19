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
import json
from typing import Dict, Optional, Tuple

# 定义本地数据存储目录
FUND_DATA_DIR = 'fund_data'
INDEX_DATA_DIR = 'index_data'
if not os.path.exists(FUND_DATA_DIR):
    os.makedirs(FUND_DATA_DIR)
if not os.path.exists(INDEX_DATA_DIR):
    os.makedirs(INDEX_DATA_DIR)
    
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

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md'):
        self.report_file = report_file
        self.output_file = output_file
        self.backtest_output_file = 'backtest_report.md'
        self.portfolio_output_file = 'portfolio_recommendation.md'
        self.fund_codes = []
        self.fund_data = {}
        self.index_code = '000300'  # 沪深300指数代码
        self.index_data = {}
        self.index_df = pd.DataFrame()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        
        # 硬编码配置参数
        self.max_workers = 5
        self.retry_attempts = 5
        self.retry_wait_seconds = 10
        self.request_timeout = 30
        self.sleep_min = 1
        self.sleep_max = 2
        
        self.min_data_points = 26
        self.max_consecutive_missing_days = 5
        self.net_value_min = 0.01
        self.net_value_max = 1000.0
        
        self.macd_fast = 12
        self.macd_slow = 26
        self.macd_signal = 9
        self.bollinger_window = 20
        self.bollinger_std = 2
        self.rsi_window = 14
        self.ma_window = 50
        self.rsi_oversold = 30
        self.rsi_overbought = 70
        self.ma_ratio_high = 1.2
        self.ma_ratio_low = 0.8
        
        self.stop_loss_percent = 0.10
        self.rsi_buy_threshold = 45
        self.rsi_sell_threshold = 65
        self.ma_ratio_buy_threshold = 1.0
        self.ma_ratio_sell_threshold = 1.2
        self.rsi_buy_strong = 35
        self.rsi_sell_weak = 65
        self.ma_ratio_strong_sell = 1.2
        self.ma_ratio_strong_buy = 0.9
        
        self.min_backtest_data = 100
        self.risk_free_rate = 0.03
        self.trading_days_per_year = 252
        
        self.max_positions = 5
        self.suggested_allocation_base = 100

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

    def _validate_fund_data(self, df: pd.DataFrame, fund_code: str) -> Tuple[bool, str]:
        """
        验证基金数据的完整性和合理性
        返回 (是否通过验证, 错误信息)
        """
        if df.empty:
            return False, f"基金 {fund_code} 数据为空"
        
        # 1. 检查必要列是否存在
        required_columns = ['date', 'net_value']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            return False, f"基金 {fund_code} 缺少必要列: {missing_columns}"
        
        # 2. 检查数据行数
        if len(df) < self.min_data_points:
            return False, f"基金 {fund_code} 数据行数不足 ({len(df)} < {self.min_data_points})"
        
        # 3. 检查日期格式和排序
        try:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            invalid_dates = df[df['date'].isna()]
            if not invalid_dates.empty:
                return False, f"基金 {fund_code} 包含 {len(invalid_dates)} 条无效日期"
            
            # 检查日期是否连续递增
            df_sorted = df.sort_values('date').reset_index(drop=True)
            date_diffs = df_sorted['date'].diff().dt.days
            consecutive_missing = (date_diffs > 1).sum()
            if consecutive_missing > self.max_consecutive_missing_days:
                logger.warning("基金 %s 存在 %d 天以上的日期断点", fund_code, consecutive_missing)
            
            # 检查是否有重复日期
            duplicates = df_sorted[df_sorted.duplicated(subset=['date'], keep=False)]
            if not duplicates.empty:
                logger.warning("基金 %s 存在 %d 条重复日期，已去重", fund_code, len(duplicates))
                df_sorted = df_sorted.drop_duplicates(subset=['date'], keep='last')
        except Exception as e:
            return False, f"基金 {fund_code} 日期解析失败: {e}"
        
        # 4. 检查净值数据
        try:
            df_sorted['net_value'] = pd.to_numeric(df_sorted['net_value'], errors='coerce')
            invalid_values = df_sorted[df_sorted['net_value'].isna()]
            if not invalid_values.empty:
                return False, f"基金 {fund_code} 包含 {len(invalid_values)} 条无效净值"
            
            # 检查净值范围
            out_of_range = df_sorted[
                (df_sorted['net_value'] < self.net_value_min) |
                (df_sorted['net_value'] > self.net_value_max)
            ]
            if not out_of_range.empty:
                logger.warning("基金 %s 存在 %d 条净值超出范围的值", fund_code, len(out_of_range))
            
            # 检查净值是否单调递增
            net_value_diffs = df_sorted['net_value'].diff()
            negative_diffs = net_value_diffs[net_value_diffs < 0]
            if not negative_diffs.empty:
                logger.warning("基金 %s 存在 %d 次净值负增长", fund_code, len(negative_diffs))
                
        except Exception as e:
            return False, f"基金 %s 净值解析失败: %s", fund_code, e
        
        # 5. 检查数据完整性
        total_days = (df_sorted['date'].max() - df_sorted['date'].min()).days
        data_coverage = len(df_sorted) / (total_days + 1) * 100
        if data_coverage < 70:  # 数据覆盖率低于70%时警告
            logger.warning("基金 %s 数据覆盖率较低: %.1f%%", fund_code, data_coverage)
        
        logger.info("基金 %s 数据验证通过: %d 行数据, 覆盖率 %.1f%%", fund_code, len(df_sorted), data_coverage)
        return True, ""

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
            self.fund_codes = sorted_codes[:1000]
            
            if not self.fund_codes:
                logger.warning("未提取到任何有效基金代码，请检查 analysis_report.md")
            else:
                logger.info("提取到 %d 个基金（测试限制前1000个）: %s", len(self.fund_codes), self.fund_codes)
            
        except Exception as e:
            logger.error("解析报告文件失败: %s", e)
            raise

    def _read_local_data(self, fund_code):
        """读取本地文件，如果存在则返回DataFrame"""
        file_path = os.path.join(FUND_DATA_DIR, f"{fund_code}.csv")
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, parse_dates=['date'])
                # 验证本地数据
                is_valid, error_msg = self._validate_fund_data(df, fund_code)
                if is_valid:
                    df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
                    logger.info("本地已存在基金 %s 数据，共 %d 行，最新日期为: %s", fund_code, len(df), df['date'].max().date())
                    return df
                else:
                    logger.warning("本地基金 %s 数据验证失败: %s，删除无效文件", fund_code, error_msg)
                    try:
                        os.remove(file_path)
                    except:
                        pass
                    return pd.DataFrame()
            except Exception as e:
                logger.warning("读取本地文件 %s 失败: %s", file_path, e)
                # 删除损坏的文件
                try:
                    os.remove(file_path)
                except:
                    pass
        return pd.DataFrame()
    
    def _load_index_data_from_file(self):
        """从本地文件加载指数数据"""
        file_path = os.path.join(INDEX_DATA_DIR, f"{self.index_code}.csv")
        if os.path.exists(file_path):
            try:
                self.index_df = pd.read_csv(file_path, parse_dates=['date'])
                self.index_df = self.index_df.sort_values(by='date', ascending=True).reset_index(drop=True)
                logger.info("大盘指数 %s 数据加载成功, 共 %d 行, 最新日期为: %s", self.index_code, len(self.index_df), self.index_df['date'].max().date())
                return True
            except Exception as e:
                logger.error("加载大盘指数数据失败: %s", e)
        return False

    def _save_to_local_file(self, fund_code, df):
        """将DataFrame保存到本地文件，覆盖旧文件"""
        # 保存前再次验证数据
        is_valid, error_msg = self._validate_fund_data(df, fund_code)
        if not is_valid:
            logger.error("保存前数据验证失败: %s", error_msg)
            return False
        
        file_path = os.path.join(FUND_DATA_DIR, f"{fund_code}.csv")
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_csv(file_path, index=False)
            logger.info("基金 %s 数据已成功保存到本地文件: %s", fund_code, file_path)
            return True
        except Exception as e:
            logger.error("保存基金 %s 数据失败: %s", fund_code, e)
            return False

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
                
                # 验证单页数据
                if not df_page.empty:
                    is_valid, error_msg = self._validate_fund_data(df_page, f"{fund_code}_page{page_index}")
                    if not is_valid:
                        logger.warning("第 %d 页数据验证失败: %s", page_index, error_msg)
                        break
                
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
                time_module.sleep(random.uniform(1, 2))  # 使用配置的sleep时间
                
            except requests.exceptions.RequestException as e:
                logger.error("基金 %s API请求失败: %s", fund_code, str(e))
                raise
            except Exception as e:
                logger.error("基金 %s API数据解析失败: %s", fund_code, str(e))
                raise

        # 合并新数据并返回
        if all_new_data:
            new_combined_df = pd.concat(all_new_data, ignore_index=True)
            # 验证合并后的数据
            is_valid, error_msg = self._validate_fund_data(new_combined_df, fund_code)
            if not is_valid:
                logger.error("合并后的新数据验证失败: %s", error_msg)
                return pd.DataFrame()
            return new_combined_df[['date', 'net_value']]
        else:
            return pd.DataFrame()

    def _calculate_indicators(self, df):
        """计算技术指标并生成结果字典"""
        if df is None or df.empty or len(df) < self.min_data_points:
            return None

        df = df.sort_values(by='date', ascending=True)
        
        # MACD - 使用配置参数
        exp_fast = df['net_value'].ewm(span=12, adjust=False).mean()
        exp_slow = df['net_value'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp_fast - exp_slow
        df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()

        # 布林带 - 使用配置参数
        df['bb_mid'] = df['net_value'].rolling(window=20, min_periods=1).mean()
        df['bb_std'] = df['net_value'].rolling(window=20, min_periods=1).std()
        df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * 2)
        df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * 2)
        
        # RSI - 使用配置参数
        delta = df['net_value'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=14, min_periods=1).mean()
        avg_loss = loss.rolling(window=14, min_periods=1).mean()
        
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi'] = 100 - (100 / (1 + rs))

        # MA - 使用配置参数
        df['ma50'] = df['net_value'].rolling(window=min(50, len(df)), min_periods=1).mean()
        df['ma_ratio'] = df['net_value'] / df['ma50']

        return df
        
    def _analyze_index(self, index_df):
        """分析大盘指数，生成技术指标和市场情绪信号"""
        if index_df.empty or len(index_df) < self.min_data_points:
            logger.warning("大盘指数数据不足，无法进行分析。")
            return None
        
        processed_index_df = self._calculate_indicators(index_df)
        latest_data = processed_index_df.iloc[-1]
        
        latest_net_value = latest_data['net_value']
        latest_rsi = latest_data['rsi']
        latest_ma_ratio = latest_data['ma_ratio']
        latest_macd_diff = latest_data['macd'] - latest_data['signal']
        
        # 生成大盘信号
        index_advice = "观察"
        if (not np.isnan(latest_rsi) and latest_rsi > 70) or \
           (not np.isnan(latest_ma_ratio) and latest_ma_ratio > 1.2):
            index_advice = "等待回调"
        elif (not np.isnan(latest_rsi) and latest_rsi < 30) or \
             (not np.isnan(latest_ma_ratio) and latest_ma_ratio < 0.8):
            index_advice = "可分批买入"
            
        index_action_signal = "持有/观察"
        if not np.isnan(latest_ma_ratio) and latest_ma_ratio < 0.95:
            index_action_signal = "强卖出/规避"
        elif (not np.isnan(latest_rsi) and latest_rsi > 65) or \
             (not np.isnan(latest_ma_ratio) and latest_ma_ratio > 1.2):
            index_action_signal = "弱卖出/规避"
        elif (not np.isnan(latest_rsi) and latest_rsi < 35) or \
             (not np.isnan(latest_ma_ratio) and latest_ma_ratio < 0.9):
            index_action_signal = "强买入"
        elif (not np.isnan(latest_rsi) and latest_rsi < 45) or \
             (not np.isnan(latest_ma_ratio) and latest_ma_ratio < 1.0):
            index_action_signal = "弱买入"

        index_signals = {
            'latest_net_value': latest_net_value,
            'rsi': latest_rsi,
            'ma_ratio': latest_ma_ratio,
            'advice': index_advice,
            'action_signal': index_action_signal
        }
        return index_signals
    
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
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < 1.0):
                action_signal = "弱买入"

            # 计算布林带位置
            bb_position = "中轨"
            if not np.isnan(latest_net_value) and not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper:
                bb_position = "上轨上方"
            elif not np.isnan(latest_net_value) and not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower:
                bb_position = "下轨下方"

            return {
                'fund_code': fund_code, 'latest_net_value': latest_net_value, 'rsi': latest_rsi, 'ma_ratio': latest_ma50_ratio,
                'macd_diff': latest_macd_diff, 'bb_upper': latest_bb_upper, 'bb_lower': latest_bb_lower, 'bb_position': bb_position,
                'advice': advice, 'action_signal': action_signal
            }
        except Exception as e:
            logger.error("处理基金 %s 时发生异常: %s", fund_code, str(e))
            return {
                'fund_code': fund_code, 'latest_net_value': "数据获取失败", 'rsi': np.nan, 'ma_ratio': np.nan,
                'macd_diff': np.nan, 'bb_upper': np.nan, 'bb_lower': np.nan, 'bb_position': 'N/A', 
                'advice': "观察", 'action_signal': 'N/A'
            }

    def generate_detailed_report(self):
        """生成详细的基金技术指标报告"""
        logger.info("开始生成详细的基金技术指标报告...")
        
        all_fund_data = []
        for fund_code in self.fund_codes:
            data = self.fund_data.get(fund_code)
            if data and data['latest_net_value'] != "数据获取失败":
                all_fund_data.append(data)
        
        if not all_fund_data:
            logger.warning("没有可用于生成报告的基金数据。")
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write(f"# 市场监控报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
                f.write("---")
                f.write("## 基金数据分析\n\n")
                f.write("❌ 未找到有效基金数据。\n\n")
            return

        all_fund_data_df = pd.DataFrame(all_fund_data)
        
        # 排序
        # 创建一个排序键
        action_signal_order = {
            '强买入': 0,
            '弱买入': 1,
            '持有/观察': 2,
            '等待回调': 3,
            '弱卖出/规避': 4,
            '强卖出/规避': 5,
            'N/A': 6
        }
        all_fund_data_df['action_signal_order'] = all_fund_data_df['action_signal'].map(action_signal_order)
        all_fund_data_df = all_fund_data_df.sort_values(by='action_signal_order', ascending=True)

        # 格式化数据以用于报告
        df_for_report = all_fund_data_df[['fund_code', 'latest_net_value', 'rsi', 'ma_ratio', 'macd_diff', 'bb_position', 'advice', 'action_signal']].copy()
        df_for_report.rename(columns={
            'fund_code': '基金代码',
            'latest_net_value': '最新净值',
            'rsi': 'RSI',
            'ma_ratio': 'MA50比例',
            'macd_diff': 'MACD信号',
            'bb_position': '布林带位置',
            'advice': '建议',
            'action_signal': '操作信号'
        }, inplace=True)
        
        df_for_report['RSI'] = df_for_report['RSI'].round(2)
        df_for_report['MA50比例'] = df_for_report['MA50比例'].round(2)
        df_for_report['最新净值'] = df_for_report['最新净值'].round(4)
        
        # 将MACD信号转换为“金叉”或“死叉”
        df_for_report['MACD信号'] = df_for_report['MACD信号'].apply(lambda x: '金叉' if x > 0 else '死叉' if x < 0 else 'N/A')
        
        markdown_table = df_for_report.to_markdown(index=False)
        
        # 获取大盘指数信号
        index_signals = self._analyze_index(self.index_df)

        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场监控报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
            f.write("---\n")
            if index_signals:
                f.write(f"### 大盘指数 {self.index_code} 市场情绪\n\n")
                f.write(f"📈 **最新净值**: {index_signals['latest_net_value']:.2f}\n")
                f.write(f"📊 **RSI**: {index_signals['rsi']:.2f}\n")
                f.write(f"📉 **MA_Ratio**: {index_signals['ma_ratio']:.2f}\n")
                f.write(f"💡 **当前信号**: {index_signals['action_signal']} | {index_signals['advice']}\n")
            f.write("---\n")
            f.write(f"## 推荐基金技术指标\n")
            f.write(f"此表格已按**行动信号优先级**排序，'强买入'基金将排在最前面。\n\n")
            f.write(markdown_table)
            f.write("\n\n")

        logger.info("详细基金技术指标报告已生成并保存到: %s", self.output_file)
        
    def _get_portfolio_signals(self, fund_data, max_positions=None):
        """根据买入信号对基金进行评分和排名"""
        if not fund_data:
            return pd.DataFrame(), pd.DataFrame()

        df = pd.DataFrame(fund_data)
        
        # 1. 过滤出具有“买入”信号的基金
        buy_signals = ['强买入', '弱买入']
        buy_candidates = df[df['action_signal'].isin(buy_signals)].copy()
        
        if buy_candidates.empty:
            return pd.DataFrame(), pd.DataFrame()

        # 2. 计算综合买入评分
        buy_candidates['buy_score'] = buy_candidates.apply(self._calculate_buy_score, axis=1)
        
        # 3. 按评分和信号排序，选出最佳基金
        buy_candidates = buy_candidates.sort_values(
            by=['action_signal', 'buy_score', 'rsi'],
            ascending=[False, False, True]
        ).reset_index(drop=True)
        
        # 根据配置限制最大持仓数
        if max_positions:
            buy_candidates = buy_candidates.head(max_positions)

        # 4. 建议分配
        if not buy_candidates.empty:
            num_positions = len(buy_candidates)
            suggested_allocation = 100 / num_positions
            buy_candidates['suggested_allocation'] = suggested_allocation
            buy_candidates['建议分配'] = buy_candidates['suggested_allocation'].apply(lambda x: f'{x:.2f} 元')

        return buy_candidates, df
    
    def _calculate_buy_score(self, data):
        """计算买入评分"""
        score = 0
        if not np.isnan(data['rsi']) and data['rsi'] < 35:
            score += 50
        elif not np.isnan(data['rsi']) and data['rsi'] < 45:
            score += 30
        
        if not np.isnan(data['ma_ratio']) and data['ma_ratio'] < 0.9:
            score += 50
        elif not np.isnan(data['ma_ratio']) and data['ma_ratio'] < 1.0:
            score += 30
            
        if not np.isnan(data['macd_diff']) and data['macd_diff'] > 0:
            score += 20
        
        return score

    def run(self):
        """主执行函数，协调整个流程"""
        try:
            # 1. 解析报告以获取基金列表
            self._parse_report()

            if not self.fund_codes:
                logger.error("无法获取基金列表，脚本终止。")
                return
            
            # 2. 加载大盘指数数据
            self._load_index_data_from_file()

            # 3. 多线程处理每个基金
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                # 使用字典存储 futures 以便映射回基金代码
                future_to_fund = {executor.submit(self._process_single_fund, fund_code): fund_code for fund_code in self.fund_codes}
                
                for future in concurrent.futures.as_completed(future_to_fund):
                    fund_code = future_to_fund[future]
                    try:
                        result = future.result()
                        if result:
                            self.fund_data[fund_code] = result
                    except Exception as e:
                        logger.error("处理基金 %s 失败: %s", fund_code, e)
            
            # 4. 生成报告
            self.generate_detailed_report()
            
        except Exception as e:
            logger.exception("脚本执行失败: %s", e)

if __name__ == '__main__':
    monitor = MarketMonitor()
    monitor.run()
