import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time
from tqdm.auto import tqdm # 用于显示进度条

# --- 核心配置 ---
# 东方财富常用的静态公共 Token (无需注册即可使用)
EASTMONEY_TOKEN = "fa5fd1943c7b386f172d6893dbfba10b"

def get_main_board_stocks():
    """
    第一步：获取当前A股股票列表，并筛选出沪深主板股票
    """
    print("正在从东方财富获取全市场股票列表...")
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "6000", # 一页拉取6000只，覆盖全A股
        "po": "1",
        "np": "1",
        "ut": EASTMONEY_TOKEN,
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23", # A股标识
        "fields": "f12,f14", # f12: 代码, f14: 名称
        "_": int(time.time() * 1000)
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    
    stock_list = []
    for item in data['data']['diff']:
        code = item['f12']
        name = item['f14']
        
        # 筛选主板并生成东财特定的 secid
        # 沪市主板：600, 601, 603, 605 开头 -> secid 前缀 1.
        # 深市主板：000, 001, 002, 003 开头 -> secid 前缀 0.
        secid = None
        if code.startswith(('600', '601', '603', '605')):
            secid = f"1.{code}"
        elif code.startswith(('000', '001', '002', '003')):
            secid = f"0.{code}"
            
        if secid:
            stock_list.append({"code": code, "name": name, "secid": secid})
            
    print(f"列表获取成功，共筛选出 {len(stock_list)} 只沪深主板股票。")
    return stock_list

def fetch_single_stock_kline(stock, target_date_str, target_date_dash):
    """
    第二步 (Worker)：获取单只股票特定日期的涨跌幅
    """
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": stock['secid'],
        "ut": EASTMONEY_TOKEN,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",     # 日K线
        "fqt": "1",       # 前复权
        "end": target_date_str, # 截止日期 YYYYMMDD
        "lmt": "1",       # 只要取离这个日期最近的1根K线即可
        "_": int(time.time() * 1000)
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        # 检查是否获取到数据
        if data['data'] and data['data']['klines']:
            kline = data['data']['klines'][0]
            # K线字段解析: 日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅(%),涨跌额,换手率
            fields = kline.split(',')
            kline_date = fields[0]
            pct_chg = float(fields[8])
            
            # 必须验证取到的K线日期是否严格等于目标日期（排除停牌股票）
            if kline_date == target_date_dash:
                return {
                    "股票代码": stock['code'],
                    "名称": stock['name'],
                    "交易日": target_date_dash,
                    "涨跌幅(%)": pct_chg
                }
    except Exception:
        pass # 忽略网络波动导致的个别失败，生产环境可加入重试机制
    return None

def get_market_extremes(target_date):
    """
    主控函数：并发拉取并筛选出涨停及跌幅超5%的股票
    :param target_date: 'YYYYMMDD'
    """
    # 格式转换：东财 API 需要对齐 'YYYY-MM-DD' 格式以验证日期
    target_date_dash = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}"
    
    # 获取主板股票池
    stocks = get_main_board_stocks()
    
    results = []
    print(f"开始并发拉取 {target_date_dash} 行情数据 (使用20个线程)...")
    
    # Kaggle 环境带宽高，20-30个线程很安全，能大幅加快速度
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 提交所有任务
        future_to_stock = {
            executor.submit(fetch_single_stock_kline, stock, target_date, target_date_dash): stock 
            for stock in stocks
        }
        
        # 使用 tqdm 进度条监控进度
        for future in tqdm(as_completed(future_to_stock), total=len(stocks), desc="拉取进度"):
            res = future.result()
            if res:
                results.append(res)
                
    # 转换为 DataFrame 进行条件过滤
    df = pd.DataFrame(results)
    
    if df.empty:
        print("未获取到数据，可能是非交易日或输入日期有误。")
        return df
        
    # 核心筛选：涨停(>= 9.9%) 或 跌幅超5%(<= -5.0%)
    cond_up = df['涨跌幅(%)'] >= 9.9
    cond_down = df['涨跌幅(%)'] <= -5.0
    
    final_df = df[cond_up | cond_down].copy()
    
    # 打标签
    final_df['表现状态'] = final_df['涨跌幅(%)'].apply(
        lambda x: '涨停' if x >= 9.9 else '跌幅超5%'
    )
    
    # 按涨跌幅降序排列
    final_df = final_df.sort_values(by='涨跌幅(%)', ascending=False).reset_index(drop=True)
    
    print(f"\n提取完成！共发现 {len(final_df)} 只符合条件的股票。")
    return final_df

# ==========================================
# 运行爬虫测试
# ==========================================
# 假设你想查询 2026年4月27日 的行情
target_trade_date = '20260427' 
df_extremes = get_market_extremes(target_trade_date)

# 在 Kaggle/Jupyter 中展示前20条
if not df_extremes.empty:
    display(df_extremes.head(20))
