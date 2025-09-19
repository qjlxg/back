import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import numpy as np
import time
import requests
from bs4 import BeautifulSoup
import re
import json
import os
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fund_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('FundAnalyzer')

class SeleniumFetcher:
    """
    使用 Selenium 模拟浏览器进行数据抓取。
    """
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # 无头模式，不显示浏览器窗口
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        # 指定 Chromium 二进制路径和 ChromeDriver 路径
        chrome_options.binary_location = os.getenv('CHROME_BINARY_PATH', '/usr/bin/chromium-browser')
        service = ChromeService(executable_path=os.getenv('CHROMEDRIVER_PATH', '/usr/bin/chromedriver'))
        try:
            # 尝试使用环境变量中的 ChromeDriver
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except WebDriverException as e:
            logger.error(f"Selenium WebDriver 初始化失败: {e}")
            self.driver = None

    def get_page_source(self, url, wait_for_element=None, timeout=30):
        if not self.driver:
            return None
        try:
            self.driver.get(url)
            if wait_for_element:
                WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_element))
                )
            return self.driver.page_source
        except (TimeoutException, WebDriverException) as e:
            logger.error(f"Selenium 抓取失败: {e}")
            return None

    def __del__(self):
        if self.driver:
            self.driver.quit()

class FundAnalyzer:
    """
    一个用于自动化分析中国公募基金的类。
    """
    def __init__(self, risk_free_rate=0.01858, cache_file='fund_cache.json', cache_data=True):
        self.fund_data = {}
        self.manager_data = {}
        self.holdings_data = {}
        self.market_data = {}
        self.report_data = []
        self.cache_file = cache_file
        self.cache_data = cache_data
        self.cache = self._load_cache()
        # 直接使用用户提供的无风险利率，不再进行抓取
        self.risk_free_rate = risk_free_rate
        self.selenium_fetcher = SeleniumFetcher()

    def _log(self, message, level='info'):
        """统一的日志记录方法"""
        if level == 'info':
            logger.info(message)
        elif level == 'warning':
            logger.warning(message)
        elif level == 'error':
            logger.error(message)

    def _load_cache(self):
        """从文件加载缓存数据"""
        if self.cache_data and os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        """将缓存数据保存到文件"""
        if self.cache_data:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=4)

    def _get_fund_data(self, fund_code: str):
        """
        获取基金的单位净值和累计净值数据，用于计算夏普比率和最大回撤。
        优先使用 akshare，失败则通过网页抓取。
        """
        if fund_code in self.cache.get('fund', {}):
            self.fund_data[fund_code] = self.cache['fund'][fund_code]
            self._log(f"使用缓存的基金 {fund_code} 数据")
            return True

        self._log(f"正在获取基金 {fund_code} 的实时数据...")
        for attempt in range(3):  # 手动重试机制，最多3次
            try:
                fund_data = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
                fund_data['净值日期'] = pd.to_datetime(fund_data['净值日期'])
                fund_data.set_index('净值日期', inplace=True)
                
                # 数据清洗：去除异常值和缺失值
                fund_data = fund_data.dropna()
                if len(fund_data) < 252:  # 至少一年数据
                    raise ValueError("数据不足，无法计算可靠的夏普比率和回撤")

                returns = fund_data['单位净值'].pct_change().dropna()
                
                annual_returns = returns.mean() * 252
                annual_volatility = returns.std() * (252**0.5)
                sharpe_ratio = (annual_returns - self.risk_free_rate) / annual_volatility if annual_volatility != 0 else 0
                
                rolling_max = fund_data['单位净值'].cummax()
                daily_drawdown = (fund_data['单位净值'] - rolling_max) / rolling_max
                max_drawdown = daily_drawdown.min() * -1
                
                self.fund_data[fund_code] = {
                    'latest_nav': float(fund_data['单位净值'].iloc[-1]),
                    'sharpe_ratio': float(sharpe_ratio),
                    'max_drawdown': float(max_drawdown)
                }
                if self.cache_data:
                    self.cache.setdefault('fund', {})[fund_code] = self.fund_data[fund_code]
                    self._save_cache()
                self._log(f"基金 {fund_code} 数据已获取：{self.fund_data[fund_code]}")
                return True
            except Exception as e:
                self._log(f"获取基金 {fund_code} 数据失败 (尝试 {attempt+1}/3): {e}")
                time.sleep(2)  # 等待2秒后重试
        self.fund_data[fund_code] = {'latest_nav': np.nan, 'sharpe_ratio': np.nan, 'max_drawdown': np.nan}
        return False

    def _scrape_manager_data_from_web(self, fund_code: str) -> dict:
        """
        从天天基金网通过网页抓取获取基金经理数据
        """
        self._log(f"尝试通过网页抓取获取基金 {fund_code} 的基金经理数据...")
        manager_url = f"http://fundf10.eastmoney.com/jjjl_{fund_code}.html"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        try:
            response = requests.get(manager_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 找到包含“基金经理变动一览”文本的标签
            title_label = soup.find('label', string='基金经理变动一览')
            if not title_label:
                self._log(f"在 {manager_url} 中未找到基金经理变动表格的标题。")
                return None
            
            # 从父容器中找到表格
            manager_table = title_label.find_parent().find_next_sibling('table')
            if not manager_table:
                self._log(f"在 {manager_url} 中未找到基金经理变动表格。")
                return None
            
            rows = manager_table.find_all('tr')
            if len(rows) < 2:
                self._log("基金经理变动表格数据不完整。")
                return None
            
            # 找到第一行数据，即最新任职的经理
            latest_manager_row = rows[1]
            cols = latest_manager_row.find_all('td')
            
            if len(cols) < 5:
                self._log("基金经理变动表格列数不正确。")
                return None
            
            manager_name = cols[2].text.strip()
            tenure_str = cols[3].text.strip()
            cumulative_return_str = cols[4].text.strip()
            
            # 解析任职天数和累计回报
            tenure_days = np.nan
            if '年又' in tenure_str:
                tenure_parts = tenure_str.split('年又')
                years = float(re.search(r'\d+', tenure_parts[0]).group())
                days = float(re.search(r'\d+', tenure_parts[1]).group())
                tenure_days = years * 365 + days
            elif '天' in tenure_str:
                tenure_days = float(re.search(r'\d+', tenure_str).group())
            elif '年' in tenure_str:
                tenure_days = float(re.search(r'\d+', tenure_str).group()) * 365
            else:
                tenure_days = np.nan
                
            cumulative_return = float(re.search(r'[-+]?\d*\.?\d+', cumulative_return_str).group()) if '%' in cumulative_return_str else np.nan

            return {
                'name': manager_name,
                'tenure_years': float(tenure_days) / 365.0 if pd.notna(tenure_days) else np.nan,
                'cumulative_return': cumulative_return
            }
        except requests.exceptions.RequestException as e:
            self._log(f"网页抓取基金 {fund_code} 经理数据失败: {e}")
            return None
        except Exception as e:
            self._log(f"解析网页内容失败: {e}")
            return None

    def get_fund_manager_data(self, fund_code: str):
        """
        获取基金经理数据（首先尝试使用 akshare，失败则通过网页抓取）
        """
        if fund_code in self.cache.get('manager', {}):
            self.manager_data[fund_code] = self.cache['manager'][fund_code]
            self._log(f"使用缓存的基金 {fund_code} 经理数据")
            return True

        self._log(f"正在获取基金 {fund_code} 的基金经理数据...")
        try:
            # 修复：akshare接口已变更为fund_manager_em
            manager_info = ak.fund_manager_em(symbol=fund_code)
            if not manager_info.empty:
                latest_manager = manager_info.sort_values(by='上任日期', ascending=False).iloc[0]
                name = latest_manager.get('姓名', 'N/A')
                tenure_days = latest_manager.get('任职天数', np.nan)
                cumulative_return = latest_manager.get('任职回报', '0%')
                cumulative_return = float(str(cumulative_return).replace('%', '')) if isinstance(cumulative_return, str) else float(cumulative_return)
                
                self.manager_data[fund_code] = {
                    'name': name,
                    'tenure_years': float(tenure_days) / 365.0 if pd.notna(tenure_days) else np.nan,
                    'cumulative_return': cumulative_return
                }
                if self.cache_data:
                    self.cache.setdefault('manager', {})[fund_code] = self.manager_data[fund_code]
                    self._save_cache()
                self._log(f"基金 {fund_code} 经理数据已通过akshare获取：{self.manager_data[fund_code]}")
                return True
        except Exception as e:
            self._log(f"使用akshare获取基金 {fund_code} 经理数据失败: {e}")

        # 如果akshare失败，尝试网页抓取
        scraped_data = self._scrape_manager_data_from_web(fund_code)
        if scraped_data:
            self.manager_data[fund_code] = scraped_data
            self._log(f"基金 {fund_code} 经理数据已通过网页抓取获取：{self.manager_data[fund_code]}")
            if self.cache_data:
                self.cache.setdefault('manager', {})[fund_code] = self.manager_data[fund_code]
                self._save_cache()
            return True
        else:
            self.manager_data[fund_code] = {'name': 'N/A', 'tenure_years': np.nan, 'cumulative_return': np.nan}
            return False

    def get_market_sentiment(self):
        """获取市场情绪（仅调用一次，基于上证指数）"""
        if self.market_data:
            self._log("使用缓存的市场情绪数据")
            return True
        self._log("正在获取市场情绪数据...")
        try:
            index_data = ak.stock_zh_index_daily_em(symbol="sh000001")
            index_data['date'] = pd.to_datetime(index_data['date'])
            last_week_data = index_data.iloc[-7:]
            
            price_change = last_week_data['close'].iloc[-1] / last_week_data['close'].iloc[0] - 1
            volume_change = last_week_data['volume'].mean() / last_week_data['volume'].iloc[:-1].mean() - 1
            if price_change > 0.01 and volume_change > 0:
                sentiment, trend = 'optimistic', 'bullish'
            elif price_change < -0.01:
                sentiment, trend = 'pessimistic', 'bearish'
            else:
                sentiment, trend = 'neutral', 'neutral'
            
            self.market_data = {'sentiment': sentiment, 'trend': trend}
            self._log(f"市场情绪数据已获取：{self.market_data}")
            return True
        except Exception as e:
            self._log(f"获取市场数据失败: {e}")
            self.market_data = {'sentiment': 'unknown', 'trend': 'unknown'}
            return False

    def get_fund_holdings_data(self, fund_code: str):
        """
        新增方法：抓取基金的股票持仓数据。
        """
        if fund_code in self.cache.get('holdings', {}):
            self.holdings_data[fund_code] = self.cache['holdings'][fund_code]
            self._log(f"使用缓存的基金 {fund_code} 持仓数据")
            return True

        self._log(f"正在获取基金 {fund_code} 的持仓数据...")
        
        # 优先使用 akshare 接口
        try:
            holdings_df = ak.fund_portfolio_hold_em(symbol=fund_code)
            if not holdings_df.empty:
                self.holdings_data[fund_code] = holdings_df.to_dict('records')
                self._log(f"基金 {fund_code} 持仓数据已通过akshare获取。")
                if self.cache_data:
                    self.cache.setdefault('holdings', {})[fund_code] = self.holdings_data[fund_code]
                    self._save_cache()
                return True
        except Exception as e:
            self._log(f"通过akshare获取基金 {fund_code} 持仓数据失败: {e}")

        # 如果 akshare 失败，尝试网页抓取
        holdings_url = f"http://fundf10.eastmoney.com/ccmx_{fund_code}.html"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            response = requests.get(holdings_url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 修复：使用更稳健的 find_next 方法，并精确匹配h4标签
            holdings_header = soup.find('h4', string=lambda t: t and '股票投资明细' in t)
            if not holdings_header:
                raise ValueError("未找到持仓表格标题。")
            
            holdings_table = holdings_header.find_next('table')
            if not holdings_table:
                raise ValueError("未找到持仓表格。")
            
            rows = holdings_table.find_all('tr')[1:] # 跳过表头
            
            holdings = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 5: # 确保列数正确
                    holdings.append({
                        '股票代码': cols[1].text.strip(),
                        '股票名称': cols[2].text.strip(),
                        '占净值比例': float(cols[4].text.strip().replace('%', '')),
                        '持仓市值（万元）': float(cols[6].text.strip().replace(',', '')),
                    })
            self.holdings_data[fund_code] = holdings
            self._log(f"基金 {fund_code} 持仓数据已通过网页抓取获取。")
            if self.cache_data:
                self.cache.setdefault('holdings', {})[fund_code] = self.holdings_data[fund_code]
                self._save_cache()
            return True
        except Exception as e:
            self._log(f"获取基金 {fund_code} 持仓数据失败: {e}")
            self.holdings_data[fund_code] = []
            return False
            
    def _evaluate_fund(self, fund_code, fund_name, fund_type):
        """
        评估单个基金的综合分数。
        """
        self._log(f"--- 正在分析基金 {fund_code} ---")
        
        # 尝试获取基本信息，如果失败则跳过整个分析
        if not self._get_fund_data(fund_code):
            self._log(f"基金 {fund_code} 基本信息获取失败，跳过分析。")
            self.report_data.append({'fund_code': fund_code, 'fund_name': fund_name, 'decision': 'Skip', 'score': np.nan})
            return
            
        # 获取基金经理数据
        self.get_fund_manager_data(fund_code)
        
        # 获取持仓数据
        self.get_fund_holdings_data(fund_code)

        # 评分体系 (示例)
        scores = {}
        values = {}
        
        # 1. 夏普比率评分 (越高越好)
        sharpe_ratio = self.fund_data[fund_code].get('sharpe_ratio')
        if pd.notna(sharpe_ratio):
            scores['sharpe_ratio_score'] = min(10, max(0, int(sharpe_ratio * 10))) # 简单线性评分
            values['sharpe_ratio_value'] = sharpe_ratio
        else:
            scores['sharpe_ratio_score'] = 0
            values['sharpe_ratio_value'] = np.nan

        # 2. 最大回撤评分 (越小越好)
        max_drawdown = self.fund_data[fund_code].get('max_drawdown')
        if pd.notna(max_drawdown):
            scores['max_drawdown_score'] = min(10, max(0, 10 - int(max_drawdown * 10))) # 简单反向线性评分
            values['max_drawdown_value'] = max_drawdown
        else:
            scores['max_drawdown_score'] = 0
            values['max_drawdown_value'] = np.nan
            
        # 3. 基金经理任职年限评分
        manager_years = self.manager_data[fund_code].get('tenure_years')
        if pd.notna(manager_years) and manager_years >= 3:
            scores['manager_years_score'] = 10
        else:
            scores['manager_years_score'] = 0
        values['manager_years_value'] = manager_years
        
        # 4. 基金经理任职回报评分
        manager_return = self.manager_data[fund_code].get('cumulative_return')
        if pd.notna(manager_return) and manager_return > 0:
            scores['manager_return_score'] = 10
        else:
            scores['manager_return_score'] = 0
        values['manager_return_value'] = manager_return
            
        # 5. 持仓集中度评分
        if self.holdings_data[fund_code]:
            holdings_df = pd.DataFrame(self.holdings_data[fund_code])
            top_10_holdings_ratio = holdings_df['占净值比例'].iloc[:10].sum()
            if top_10_holdings_ratio < 60:
                scores['holding_concentration_score'] = 10
            else:
                scores['holding_concentration_score'] = 5
            values['holding_concentration_value'] = top_10_holdings_ratio
        else:
            scores['holding_concentration_score'] = 0
            values['holding_concentration_value'] = np.nan
        
        # 其他评分项...
        scores['fund_type_score'] = 10 if '股票型' in fund_type or '混合型' in fund_type else 5
        scores['market_sentiment_adj_score'] = 5 if self.market_data.get('trend') == 'bullish' and scores.get('sharpe_ratio_score', 0) > 5 else 0
        
        total_score = sum(scores.values())
        
        # 决策逻辑
        decision = '推荐' if total_score > 30 else '观望'
        
        self.report_data.append({
            'fund_code': fund_code,
            'fund_name': fund_name,
            'decision': decision,
            'score': total_score,
            'scores_details': scores,
            'values_details': values
        })
        self._log(f"评分详情: {scores}")
        self._log(f"基金 {fund_code} 评估完成，总分: {total_score}，决策: {decision}")

    def _save_report_to_markdown(self):
        """将分析报告保存为 Markdown 文件"""
        if not self.report_data:
            return
        
        report_path = "analysis_report.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("--- 批量基金分析报告 ---\n\n")
            f.write(f"生成日期: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("--- 汇总结果 ---\n\n")
            
            results_df = pd.DataFrame(self.report_data)
            valid_results = results_df[results_df['decision'] != 'Skip']
            
            if not valid_results.empty:
                f.write("### 推荐基金\n\n")
                recommended = valid_results[valid_results['decision'] == '推荐'].sort_values(by='score', ascending=False)
                if not recommended.empty:
                    f.write(recommended[['fund_code', 'fund_name', 'score']].to_markdown(index=False) + "\n\n")
                else:
                    f.write("无\n\n")
                    
                f.write("### 观望基金\n\n")
                watchlist = valid_results[valid_results['decision'] == '观望'].sort_values(by='score', ascending=False)
                if not watchlist.empty:
                    f.write(watchlist[['fund_code', 'fund_name', 'score']].to_markdown(index=False) + "\n\n")
                else:
                    f.write("无\n\n")
            
            f.write("--- 详细分析 ---\n\n")
            for item in self.report_data:
                f.write(f"### 基金 {item['fund_code']} - {item.get('fund_name', 'N/A')}\n")
                f.write(f"- 最终决策: **{item['decision']}**\n")
                f.write(f"- 综合分数: **{item['score']:.2f}**\n")
                
                if item['decision'] != 'Skip':
                    f.write("- **评分细项**:\n")
                    for k, v in item.get('scores_details', {}).items():
                        f.write(f"  - {k}: {v}\n")
                    f.write("- **数据值**:\n")
                    for k, v in item.get('values_details', {}).items():
                        f.write(f"  - {k}: {v}\n")
                f.write("\n---\n\n")

    def run_analysis(self, fund_codes: list, fund_info: dict):
        """
        运行批量基金分析的主函数。
        """
        self._log("--- 批量基金分析启动 ---")
        
        # 仅调用一次获取市场情绪
        self.get_market_sentiment()
        
        for code in fund_codes:
            self._evaluate_fund(code, fund_info.get(code, 'N/A'), '混合型') # 假设类型
        
        # 生成并保存最终报告
        results_df = pd.DataFrame(self.report_data)
        if not results_df.empty and 'decision' in results_df.columns:
            self._log("\n--- 全部基金分析结果 ---")
            self._log("\n" + results_df[['decision', 'score', 'fund_code', 'fund_name']].to_markdown(index=False))
        else:
            self._log("\n没有基金获得有效评分。")
        
        self._save_report_to_markdown()
        
        return results_df

if __name__ == '__main__':
    # 请确保已安装所有库，特别是 Selenium 和 ChromeDriver
    # pip install selenium akshare pandas numpy requests beautifulsoup4 lxml
    # 还需要手动下载与您的 Chrome 版本匹配的 ChromeDriver 并配置环境变量或修改路径
    
    funds_list_url = 'https://raw.githubusercontent.com/qjlxg/rep/main/recommended_cn_funds.csv'
    
    try:
        logger.info("正在从 CSV 导入基金代码列表...")
        df_funds = pd.read_csv(funds_list_url, encoding='gb18030')
        
        # 修正列名引用
        fund_codes_to_analyze = [str(code).zfill(6) for code in df_funds['代码'].unique().tolist()]
        fund_info_dict = dict(zip(fund_codes_to_analyze, df_funds['名称'].tolist()))
        
        logger.info(f"导入成功，共 {len(fund_codes_to_analyze)} 个基金代码")
    except Exception as e:
        logger.error(f"导入基金列表失败: {e}")
        fund_codes_to_analyze = []
        fund_info_dict = {}
    
    if fund_codes_to_analyze:
        test_fund_codes = fund_codes_to_analyze[:1500] # 减少测试基金数量，避免频繁网络请求
        logger.info(f"分析前 {len(test_fund_codes)} 个基金：{test_fund_codes}...")
        analyzer = FundAnalyzer()
        analyzer.run_analysis(test_fund_codes, fund_info_dict)
    else:
        logger.info("没有基金列表可供分析，程序结束。")
