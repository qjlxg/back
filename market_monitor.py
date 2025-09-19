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

# 配置外部化 - 从配置文件加载参数
CONFIG_FILE = 'market_monitor_config.json'

def load_config():
    """加载配置文件，如果不存在则创建默认配置"""
    default_config = {
        "network": {
            "max_workers": 5,
            "retry_attempts": 5,
            "retry_wait_seconds": 10,
            "request_timeout": 30,
            "sleep_min": 1,
            "sleep_max": 2
        },
        "data_validation": {
            "min_data_points": 26,
            "max_consecutive_missing_days": 5,
            "net_value_min": 0.01,
            "net_value_max": 1000.0,
            "date_format": "%Y-%m-%d"
        },
        "technical_indicators": {
            "macd_fast": 12,
            "macd_slow": 26,
            "macd_signal": 9,
            "bollinger_window": 20,
            "bollinger_std": 2,
            "rsi_window": 14,
            "ma_window": 50,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "ma_ratio_high": 1.2,
            "ma_ratio_low": 0.8
        },
        "trading_signals": {
            "stop_loss_percent": 0.10,
            "rsi_buy_threshold": 45,
            "rsi_sell_threshold": 65,
            "ma_ratio_buy_threshold": 1.0,
            "ma_ratio_sell_threshold": 1.2,
            "rsi_buy_strong": 35,
            "rsi_sell_weak": 65,
            "ma_ratio_strong_sell": 1.2,
            "ma_ratio_strong_buy": 0.9
        },
        "backtest": {
            "min_backtest_data": 100,
            "risk_free_rate": 0.03,
            "trading_days_per_year": 252
        },
        "portfolio": {
            "max_positions": 5,
            "suggested_allocation_base": 100
        }
    }
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
            # 合并默认配置和现有配置（现有配置优先）
            merged_config = {}
            for section, default_values in default_config.items():
                merged_config[section] = default_values.copy()
                if section in config:
                    merged_config[section].update(config[section])
            logger.info("配置文件 %s 加载成功", CONFIG_FILE)
            return merged_config
        except Exception as e:
            logger.warning("加载配置文件失败: %s，使用默认配置", e)
    else:
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            logger.info("已创建默认配置文件 %s", CONFIG_FILE)
        except Exception as e:
            logger.warning("创建配置文件失败: %s", e)
    
    return default_config

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

# 加载配置
CONFIG = load_config()

# 定义本地数据存储目录
FUND_DATA_DIR = 'fund_data'
INDEX_DATA_DIR = 'index_data'
if not os.path.exists(FUND_DATA_DIR):
    os.makedirs(FUND_DATA_DIR)
if not os.path.exists(INDEX_DATA_DIR):
    os.makedirs(INDEX_DATA_DIR)

class MarketMonitor:
    def __init__(self, report_file='analysis_report.md', output_file='market_monitor_report.md', backtest_output_file='backtest_report.md'):
        self.report_file = report_file
        self.output_file = output_file
        self.backtest_output_file = backtest_output_file
        self.portfolio_output_file = 'portfolio_recommendation.md'
        self.fund_codes = []
        self.fund_data = {}
        self.index_code = '000300'  # 沪深300指数代码
        self.index_data = {}
        self.index_df = pd.DataFrame()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        # 从配置中加载参数
        self.max_workers = CONFIG['network']['max_workers']
        self.retry_attempts = CONFIG['network']['retry_attempts']
        self.retry_wait_seconds = CONFIG['network']['retry_wait_seconds']
        self.request_timeout = CONFIG['network']['request_timeout']
        self.sleep_min = CONFIG['network']['sleep_min']
        self.sleep_max = CONFIG['network']['sleep_max']
        self.min_data_points = CONFIG['data_validation']['min_data_points']
        self.max_consecutive_missing_days = CONFIG['data_validation']['max_consecutive_missing_days']
        self.net_value_min = CONFIG['data_validation']['net_value_min']
        self.net_value_max = CONFIG['data_validation']['net_value_max']
        self.macd_fast = CONFIG['technical_indicators']['macd_fast']
        self.macd_slow = CONFIG['technical_indicators']['macd_slow']
        self.macd_signal = CONFIG['technical_indicators']['macd_signal']
        self.bollinger_window = CONFIG['technical_indicators']['bollinger_window']
        self.bollinger_std = CONFIG['technical_indicators']['bollinger_std']
        self.rsi_window = CONFIG['technical_indicators']['rsi_window']
        self.ma_window = CONFIG['technical_indicators']['ma_window']
        self.rsi_oversold = CONFIG['technical_indicators']['rsi_oversold']
        self.rsi_overbought = CONFIG['technical_indicators']['rsi_overbought']
        self.ma_ratio_high = CONFIG['technical_indicators']['ma_ratio_high']
        self.ma_ratio_low = CONFIG['technical_indicators']['ma_ratio_low']
        self.stop_loss_percent = CONFIG['trading_signals']['stop_loss_percent']
        self.rsi_buy_threshold = CONFIG['trading_signals']['rsi_buy_threshold']
        self.rsi_sell_threshold = CONFIG['trading_signals']['rsi_sell_threshold']
        self.ma_ratio_buy_threshold = CONFIG['trading_signals']['ma_ratio_buy_threshold']
        self.ma_ratio_sell_threshold = CONFIG['trading_signals']['ma_ratio_sell_threshold']
        self.rsi_buy_strong = CONFIG['trading_signals']['rsi_buy_strong']
        self.rsi_sell_weak = CONFIG['trading_signals']['rsi_sell_weak']
        self.ma_ratio_strong_sell = CONFIG['trading_signals']['ma_ratio_strong_sell']
        self.ma_ratio_strong_buy = CONFIG['trading_signals']['ma_ratio_strong_buy']
        self.min_backtest_data = CONFIG['backtest']['min_backtest_data']
        self.risk_free_rate = CONFIG['backtest']['risk_free_rate']
        self.trading_days_per_year = CONFIG['backtest']['trading_days_per_year']
        self.max_positions = CONFIG['portfolio']['max_positions']
        self.suggested_allocation_base = CONFIG['portfolio']['suggested_allocation_base']

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
            return False, f"基金 {fund_code} 净值解析失败: {e}"
        
        # 5. 检查数据完整性
        total_days = (df_sorted['date'].max() - df_sorted['date'].min()).days
        data_coverage = len(df_sorted) / (total_days + 1) * 100
        if data_coverage < 70:  # 数据覆盖率低于70%时警告
            logger.warning("基金 %s 数据覆盖率较低: %.1f%%", fund_code, data_coverage)
        
        logger.info("基金 %s 数据验证通过: %d 行数据, 覆盖率 %.1f%%", 
                   fund_code, len(df_sorted), data_coverage)
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
        stop=tenacity.stop_after_attempt(lambda: CONFIG['network']['retry_attempts']),
        wait=tenacity.wait_fixed(lambda: CONFIG['network']['retry_wait_seconds']),
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
                response = requests.get(url, headers=self.headers, timeout=self.request_timeout)
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
                time_module.sleep(random.uniform(self.sleep_min, self.sleep_max))  # 使用配置的sleep时间
                
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

    def _load_index_data_from_file(self):
        """
        从本地文件加载大盘指数数据
        """
        file_path = os.path.join(INDEX_DATA_DIR, f"{self.index_code}.csv")
        logger.info("正在从本地文件 %s 加载大盘指数数据...", file_path)
        if not os.path.exists(file_path):
            logger.error("本地大盘指数文件 %s 不存在，请运行 download_index_data.py 下载。", file_path)
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(file_path, parse_dates=['date'])
            # 验证指数数据
            is_valid, error_msg = self._validate_fund_data(df, self.index_code)
            if not is_valid:
                logger.error("大盘指数数据验证失败: %s", error_msg)
                return pd.DataFrame()
            
            df = df.sort_values(by='date', ascending=True).reset_index(drop=True)
            logger.info("大盘指数 %s 数据加载成功，共 %d 行，最新日期为: %s", self.index_code, len(df), df['date'].max().date())
            return df
        except Exception as e:
            logger.error("加载本地大盘指数文件 %s 失败: %s", file_path, e)
            return pd.DataFrame()

    def _calculate_indicators(self, df):
        """计算技术指标并生成结果字典"""
        if df is None or df.empty or len(df) < self.min_data_points:
            return None

        df = df.sort_values(by='date', ascending=True)
        
        # MACD - 使用配置参数
        exp_fast = df['net_value'].ewm(span=self.macd_fast, adjust=False).mean()
        exp_slow = df['net_value'].ewm(span=self.macd_slow, adjust=False).mean()
        df['macd'] = exp_fast - exp_slow
        df['signal'] = df['macd'].ewm(span=self.macd_signal, adjust=False).mean()

        # 布林带 - 使用配置参数
        df['bb_mid'] = df['net_value'].rolling(window=self.bollinger_window, min_periods=1).mean()
        df['bb_std'] = df['net_value'].rolling(window=self.bollinger_window, min_periods=1).std()
        df['bb_upper'] = df['bb_mid'] + (df['bb_std'] * self.bollinger_std)
        df['bb_lower'] = df['bb_mid'] - (df['bb_std'] * self.bollinger_std)
        
        # RSI - 使用配置参数
        delta = df['net_value'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=self.rsi_window, min_periods=1).mean()
        avg_loss = loss.rolling(window=self.rsi_window, min_periods=1).mean()
        
        rs = avg_gain / avg_loss.replace(0, np.nan)
        df['rsi'] = 100 - (100 / (1 + rs))

        # MA - 使用配置参数
        df['ma50'] = df['net_value'].rolling(window=min(self.ma_window, len(df)), min_periods=1).mean()
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
            if (not np.isnan(latest_rsi) and latest_rsi > self.rsi_overbought) or \
               (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
               (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > self.ma_ratio_high):
                advice = "等待回调"
            elif (not np.isnan(latest_rsi) and latest_rsi < self.rsi_oversold) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < self.ma_ratio_low):
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
            elif (not np.isnan(latest_rsi) and latest_rsi > self.rsi_overbought) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > self.ma_ratio_strong_sell) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff < 0):
                action_signal = "强卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi > self.rsi_sell_weak) or \
                 (not np.isnan(latest_bb_upper) and latest_net_value > latest_bb_upper) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio > self.ma_ratio_strong_sell):
                action_signal = "弱卖出/规避"
            elif (not np.isnan(latest_rsi) and latest_rsi < self.rsi_buy_strong) and \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < self.ma_ratio_strong_buy) and \
                 (not np.isnan(latest_macd_diff) and latest_macd_diff > 0):
                action_signal = "强买入"
            elif (not np.isnan(latest_rsi) and latest_rsi < self.rsi_buy_threshold) or \
                 (not np.isnan(latest_bb_lower) and latest_net_value < latest_bb_lower) or \
                 (not np.isnan(latest_ma50_ratio) and latest_ma50_ratio < self.ma_ratio_buy_threshold):
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

    def _analyze_index(self, index_df):
        """分析大盘指数，计算技术指标和信号"""
        logger.info("开始分析大盘指数 %s...", self.index_code)
        if not index_df.empty:
            self.index_df = index_df
            result = self._get_latest_signals(self.index_code, self.index_df.tail(100))
            self.index_data = result
            logger.info("大盘指数 %s 分析完成", self.index_code)
        else:
            logger.error("大盘指数 %s 未获取到任何有效数据", self.index_code)
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

    def _get_portfolio_signals(self, fund_data, max_positions=None):
        """
        根据综合评分筛选出值得买入的基金
        """
        if max_positions is None:
            max_positions = self.max_positions
            
        buy_signals = []
        for code, data in fund_data.items():
            if data['action_signal'] in ["强买入", "弱买入"] and not np.isnan(data['rsi']):
                score = self._calculate_buy_score(data)
                buy_signals.append({
                    'code': code,
                    'signal': data['action_signal'],
                    'score': score,
                    'rsi': data['rsi'],
                    'ma_ratio': data['ma_ratio']
                })
        
        # 按照评分降序排列，取前N个
        buy_signals = sorted(buy_signals, key=lambda x: x['score'], reverse=True)
        
        return buy_signals[:max_positions]

    def _calculate_buy_score(self, data):
        """
        计算基金买入评分
        RSI越低分数越高，MA_Ratio越低分数越高，布林带位置越低分数越高
        """
        score = 0
        
        # 1. RSI评分: 40分 - 使用配置的阈值
        if data['rsi'] < self.rsi_oversold:
            score += 40
        elif data['rsi'] < self.rsi_buy_strong:
            score += 30
        elif data['rsi'] < self.rsi_buy_threshold:
            score += 20
        
        # 2. MA_Ratio评分: 40分 - 使用配置的阈值
        if data['ma_ratio'] < self.ma_ratio_strong_buy:
            score += 40
        elif data['ma_ratio'] < self.ma_ratio_low:
            score += 30
        elif data['ma_ratio'] < self.ma_ratio_buy_threshold:
            score += 20
        elif data['ma_ratio'] < 1.05:
            score += 10

        # 3. 布林带位置评分: 20分
        bb_position = data['bb_position']
        if bb_position == "下轨下方":
            score += 20
        elif bb_position == "中轨":
            score += 10
        else:
            score += 5
        
        return score

    def generate_portfolio_recommendation(self):
        """生成投资组合推荐"""
        buy_candidates = self._get_portfolio_signals(self.fund_data, max_positions=3)
        
        with open(self.portfolio_output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 投资组合推荐报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
            f.write("---")
            f.write(f"### 大盘指数 {self.index_code} 市场情绪\n\n")
            f.write(f"📈 **最新净值**: {float(self.index_data['latest_net_value']):.2f}\n")
            f.write(f"📊 **RSI**: {self.index_data['rsi']:.2f}\n")
            f.write(f"📉 **MA_Ratio**: {self.index_data['ma_ratio']:.2f}\n")
            f.write(f"💡 **当前信号**: {self.index_data['action_signal']} | {self.index_data['advice']}\n")
            f.write("---")
            f.write("\n## 推荐基金列表\n\n")
            
            if buy_candidates:
                f.write("| 序号 | 信号 | 基金代码 | 评分 | RSI | MA_Ratio |\n")
                f.write("|------|------|----------|------|-----|----------|\n")
                for i, candidate in enumerate(buy_candidates, 1):
                    signal_emoji = "🟢 强买入" if candidate['signal'] == "强买入" else "🟡 弱买入"
                    f.write(f"| {i} | {signal_emoji} | {candidate['code']} | {candidate['score']:.0f} | {candidate['rsi']:.1f} | {candidate['ma_ratio']:.2f} |\n")
                
                if buy_candidates:
                    suggested_amount = buy_candidates[0]['score'] // 10 * self.suggested_allocation_base
                    f.write(f"\n## 建议分配\n")
                    f.write(f"💰 建议每支基金分配: {suggested_amount} 元\n\n")
                    f.write(f"📈 今日买入机会: {len(buy_candidates)} / {len(self.fund_codes)}\n\n")
            else:
                f.write("## 推荐结果\n")
                f.write("❌ 今日无符合条件的买入机会，建议观望\n\n")
                f.write(f"📊 总扫描基金数: {len(self.fund_codes)}\n\n")
        
        logger.info("投资组合推荐报告生成完成: %s", self.portfolio_output_file)

    def generate_detailed_report(self):
        """生成详细报告"""
        logger.info("正在生成详细报告: %s", self.output_file)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(f"# 市场监控报告 ({datetime.now().strftime('%Y-%m-%d')})\n\n")
            f.write("---")
            f.write(f"### 大盘指数 {self.index_code} 市场情绪\n\n")
            f.write(f"📈 **最新净值**: {self.index_data['latest_net_value']:.2f}\n")
            f.write(f"📊 **RSI**: {self.index_data['rsi']:.2f}\n")
            f.write(f"📉 **MA_Ratio**: {self.index_data['ma_ratio']:.2f}\n")
            f.write(f"💡 **当前信号**: {self.index_data['action_signal']} | {self.index_data['advice']}\n")
            f.write("---")
            f.write("\n## 基金数据分析\n\n")
            f.write("| 基金代码 | 最新净值 | RSI | MA50比例 | MACD信号 | 布林带位置 | 建议 | 操作信号 |\n")
            f.write("|----------|----------|-----|----------|----------|------------|------|----------|\n")
            
            for code, data in self.fund_data.items():
                line = (
                    f"| {data['fund_code']} | {data['latest_net_value']:.4f} | {data['rsi']:.2f} | {data['ma_ratio']:.2f} | "
                    f"{'金叉' if data['macd_diff'] > 0 else '死叉' if data['macd_diff'] < 0 else '无信号'} | "
                    f"{data['bb_position']} | {data['advice']} | {data['action_signal']} |\n"
                )
                f.write(line)
        
        logger.info("详细报告生成完成: %s", self.output_file)

    def generate_backtest_report(self):
        """生成回测报告"""
        logger.info("开始生成回测报告: %s", self.backtest_output_file)
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_code = {executor.submit(self._run_backtest_for_fund, code): code for code in self.fund_codes}
            for future in concurrent.futures.as_completed(future_to_code):
                fund_code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error("回测基金 %s 时发生异常: %s", fund_code, e)
        
        if not results:
            logger.warning("没有可用于回测的基金数据。")
            with open(self.backtest_output_file, 'w', encoding='utf-8') as f:
                f.write("# 历史回测报告\n\n")
                f.write("---")
                f.write("\n\n❌ 没有可用于回测的基金数据。\n")
            return
        
        df_results = pd.DataFrame(results)
        df_results.sort_values(by='cagr', ascending=False, inplace=True)
        
        # 保存详细的回测结果到CSV
        df_results.to_csv('backtest_results.csv', index=False, float_format='%.4f')

        with open(self.backtest_output_file, 'w', encoding='utf-8') as f:
            f.write("# 历史回测报告\n\n")
            f.write("---")
            f.write("\n\n## 综合表现排名 (按年化收益率)\n\n")
            f.write(df_results.to_markdown(index=False, floatfmt=".2f"))
        
        logger.info("回测报告生成完成: %s", self.backtest_output_file)
    
    def _run_backtest_for_fund(self, fund_code):
        df = self._read_local_data(fund_code)
        if df.empty or len(df) < self.min_backtest_data:
            logger.warning(f"基金 {fund_code} 数据不足，无法回测。")
            return None
        
        backtest_result = self._backtest_strategy(fund_code, df)
        backtest_result['fund_code'] = fund_code
        logger.info(f"基金 {fund_code} 回测结果: 累计回报={backtest_result['cum_return']:.2f}, 最大回撤={backtest_result['max_drawdown']:.2f}, 夏普比率={backtest_result['sharpe_ratio']:.2f}, 胜率={backtest_result['win_rate']:.2f}, 年化收益率={backtest_result['cagr']:.2f}, 交易次数={backtest_result['total_trades']}")
        
        return backtest_result

    def _backtest_strategy(self, fund_code, df):
        """历史回测策略性能"""
        if df is None or df.empty or len(df) < self.min_backtest_data:
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

            # 止损逻辑 - 使用配置参数
            if position == 1 and (latest_net_value / buy_price) < (1 - self.stop_loss_percent):
                sell_price = latest_net_value
                ret = (sell_price - buy_price) / buy_price
                trades.append({'buy_date': df.iloc[i-1]['date'], 'sell_date': df.iloc[i]['date'], 'return': ret, 'type': 'stop_loss'})
                position = 0
                buy_price = 0
                continue # 继续下一天

            # 交易信号逻辑 - 使用配置参数
            latest_rsi = latest_data['rsi']
            latest_ma_ratio = latest_data['ma_ratio']
            latest_macd_diff = latest_data['macd'] - latest_data['signal']
            
            # 买入条件：RSI低于阈值或MA_Ratio低于阈值，且MACD金叉
            if position == 0 and \
               (latest_rsi < self.rsi_buy_threshold or latest_ma_ratio < self.ma_ratio_buy_threshold) and \
               (df.iloc[i-1]['macd'] - df.iloc[i-1]['signal'] <= 0 and latest_macd_diff > 0):
                
                position = 1
                buy_price = latest_net_value
                
            # 卖出条件：RSI高于阈值或MA_Ratio高于阈值，且MACD死叉
            elif position == 1 and \
                 (latest_rsi > self.rsi_sell_threshold or latest_ma_ratio > self.ma_ratio_sell_threshold) and \
                 (df.iloc[i-1]['macd'] - df.iloc[i-1]['signal'] >= 0 and latest_macd_diff < 0):
                
                sell_price = latest_net_value
                ret = (sell_price - buy_price) / buy_price
                trades.append({'buy_date': df.iloc[i-1]['date'], 'sell_date': df.iloc[i]['date'], 'return': ret, 'type': 'normal'})
                position = 0
                buy_price = 0
        
        # 如果回测结束时仍有持仓，则以最后一天净值清仓
        if position == 1:
            sell_price = df.iloc[-1]['net_value']
            ret = (sell_price - buy_price) / buy_price
            trades.append({'buy_date': buy_price, 'sell_date': sell_price, 'return': ret, 'type': 'final_sell'})

        # 计算回测指标
        if not trades:
            return {"cum_return": np.nan, "max_drawdown": np.nan, "sharpe_ratio": np.nan, "win_rate": np.nan, "cagr": np.nan, "total_trades": 0}

        cum_return = np.product([1 + t['return'] for t in trades]) - 1
        
        equity_series = pd.Series(equity)
        max_drawdown = (equity_series / equity_series.cummax() - 1).min()
        
        win_trades = [t for t in trades if t['return'] > 0]
        win_rate = len(win_trades) / len(trades) if trades else 0
        
        daily_returns = df['net_value'].pct_change().dropna()
        if daily_returns.empty:
             sharpe_ratio = np.nan
             cagr = np.nan
        else:
            daily_risk_free = self.risk_free_rate / self.trading_days_per_year
            sharpe_ratio = (daily_returns.mean() - daily_risk_free) / daily_returns.std() * np.sqrt(self.trading_days_per_year)
            
            # 计算年化收益率 (CAGR)
            start_date = df['date'].iloc[0]
            end_date = df['date'].iloc[-1]
            total_years = (end_date - start_date).days / 365.25
            cagr = ((1 + cum_return) ** (1 / total_years)) - 1 if total_years > 0 else 0

        return {
            "cum_return": cum_return,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "win_rate": win_rate,
            "cagr": cagr,
            "total_trades": len(trades)
        }

    def run(self):
        """主执行流程"""
        try:
            # 步骤 1: 加载本地指数数据
            index_df = self._load_index_data_from_file()
            self._analyze_index(index_df)

            # 步骤 2: 解析推荐基金代码
            self._parse_report()
            if not self.fund_codes:
                logger.error("没有提取到任何基金代码，无法继续处理")
                return

            # 步骤 3: 预加载本地基金数据并检查是否需要下载
            logger.info("开始预加载本地缓存数据...")
            fund_codes_to_fetch = []
            expected_latest_date = self._get_expected_latest_date()
            for fund_code in self.fund_codes:
                local_df = self._read_local_data(fund_code)
                if not local_df.empty:
                    latest_local_date = local_df['date'].max().date()
                    data_points = len(local_df)
                    # 检查数据是否最新且完整
                    if latest_local_date >= expected_latest_date and data_points >= self.min_data_points:
                        logger.info("基金 %s 的本地数据已是最新 (%s, 期望: %s) 且数据量足够 (%d 行)，直接加载。", fund_code, latest_local_date, expected_latest_date, data_points)
                        self.fund_data[fund_code] = self._get_latest_signals(fund_code, local_df.tail(100))
                        continue
                    else:
                        if latest_local_date < expected_latest_date:
                            logger.info("基金 %s 本地数据已过时（最新日期为 %s，期望 %s），需要从网络获取新数据。", fund_code, latest_local_date, expected_latest_date)
                        if data_points < self.min_data_points:
                            logger.info("基金 %s 本地数据量不足（仅 %d 行，需至少 %d 行），需要从网络获取。", fund_code, data_points, self.min_data_points)
                else:
                    logger.info("基金 %s 本地数据不存在，需要从网络获取。", fund_code)
                fund_codes_to_fetch.append(fund_code)
            
            # 步骤 4: 多线程网络下载和处理
            if fund_codes_to_fetch:
                logger.info("开始使用多线程获取 %d 个基金的新数据...", len(fund_codes_to_fetch))
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
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
            else:
                logger.info("所有基金数据均来自本地缓存，无需网络下载。")

            if len(self.fund_data) > 0:
                logger.info("所有基金数据处理完成。")
            else:
                logger.error("所有基金数据均获取失败。")
            
            # 步骤 5: 生成报告
            self.generate_portfolio_recommendation()
            self.generate_detailed_report()
            self.generate_backtest_report()

        except Exception as e:
            logger.exception("脚本执行失败: %s", e)

    def _process_single_fund(self, fund_code):
        """处理单个基金数据：读取本地，下载增量，合并，保存，并计算信号"""
        local_df = self._read_local_data(fund_code)
        latest_local_date = local_df['date'].max().date() if not local_df.empty else None
        
        new_df = self._fetch_fund_data(fund_code, latest_local_date)
        
        if not new_df.empty:
            df_final = pd.concat([local_df, new_df]).drop_duplicates(subset=['date'], keep='last').sort_values(by='date', ascending=True)
            if self._save_to_local_file(fund_code, df_final):
                return self._get_latest_signals(fund_code, df_final.tail(100))
            else:
                logger.error("基金 %s 保存失败，跳过", fund_code)
                return None
        elif not local_df.empty:
            # 如果没有新数据，且本地有数据，则使用本地数据计算信号
            logger.info("基金 %s 无新数据，使用本地历史数据进行分析", fund_code)
            return self._get_latest_signals(fund_code, local_df.tail(100))
        else:
            # 如果既没有新数据，本地又没有数据，则返回失败
            logger.error("基金 %s 未获取到任何有效数据，且本地无缓存", fund_code)
            return None

if __name__ == "__main__":
    try:
        logger.info("脚本启动")
        monitor = MarketMonitor()
        monitor.run()
        logger.info("脚本运行结束")
    except Exception as e:
        logger.exception("脚本运行失败: %s", e)
