import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
# Colab 中内置了 tqdm，用于显示美观的进度条
from tqdm.notebook import tqdm 

def get_mainboard_stocks():
    """
    第一步：从东方财富获取所有A股股票，并筛选出主板（沪市60开头，深市00开头）
    """
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    # fs 参数包含了沪深京A股的过滤条件
    params = {
        "pn": "1",
        "pz": "5000", # 一页获取5000条，足以覆盖所有A股
        "po": "1",
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    stocks = data['data']['diff']
    
    mainboard_stocks = []
    for stock in stocks:
        code = stock['f12']
        name = stock['f14']
        
        # 筛选主板股票并判断市场标识：沪市(60开头)前缀为1，深市(00开头)前缀为0
        if str(code).startswith("60"):
            market = "1"
        elif str(code).startswith("00"):
            market = "0"
        else:
            continue # 排除创业板(300)、科创板(688)等
            
        mainboard_stocks.append({
            "code": code,
            "name": name,
            "secid": f"{market}.{code}" # 东方财富特定格式：市场号.股票代码
        })
        
    return mainboard_stocks

def get_target_day_data(stock, target_date):
    """
    第二步：获取单只股票特定交易日的K线数据，并判断是否满足条件
    """
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    date_str = target_date.replace("-", "")
    
    params = {
        "secid": stock["secid"],
        "fields1": "f1,f2,f3,f4,f5,f6",
        # f51:日期, f53:收盘价, f59:涨跌幅
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101", # 日K
        "fqt": "1",   # 前复权
        "beg": date_str,
        "end": date_str
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        data = response.json()
        
        # 判断当天是否有交易数据（停牌或非交易日会返回空）
        if data.get('data') and data['data'].get('klines'):
            kline = data['data']['klines'][0].split(',')
            date = kline[0]
            pct_change = float(kline[8]) # 获取当天的涨跌幅(%)
            
            # 动态判断涨停阈值 (ST股涨跌幅限制为5%，普通主板为10%)
            is_st = 'ST' in stock["name"]
            limit_up_threshold = 4.9 if is_st else 9.9
            
            # 条件判断：涨停 或 下跌超5%
            is_limit_up = pct_change >= limit_up_threshold
            is_drop_over_5 = pct_change <= -5.0
            
            if is_limit_up or is_drop_over_5:
                return {
                    "股票代码": stock["code"],
                    "股票名称": stock["name"],
                    "交易日": date,
                    "涨跌幅(%)": pct_change,
                    "状态": "涨停" if is_limit_up else "跌幅超5%"
                }
    except Exception:
        pass # 忽略请求超时或解析错误
    return None

def fetch_market_abnormal_stocks(target_date):
    """
    第三步：使用多线程并发加速爬取过程
    """
    stocks = get_mainboard_stocks()
    print(f"✅ 成功获取主板股票代码：{len(stocks)}只。")
    print(f"⏳ 开始抓取 {target_date} 的行情数据 (可能需要1-2分钟)...")
    
    results = []
    # 设置最大线程数为20，既能加速爬取，又不会触发东方财富的安全拦截
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_stock = {executor.submit(get_target_day_data, stock, target_date): stock for stock in stocks}
        
        # 配合 tqdm 显示进度条
        for future in tqdm(as_completed(future_to_stock), total=len(stocks), desc="爬取进度"):
            res = future.result()
            if res:
                results.append(res)
                
    # 转换为 Pandas DataFrame 并按涨跌幅降序排列
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by="涨跌幅(%)", ascending=False).reset_index(drop=True)
        print(f"\n🎉 爬取完成！共有 {len(df)} 只股票符合条件。")
    else:
        print("\n⚠️ 爬取完成！该交易日没有符合条件的股票（请检查日期是否为周末/节假日）。")
        
    return df

# =========================================
# 在此处修改为你想要查询的任意交易日
# =========================================
TARGET_DATE = "2023-11-10"  # 格式必须为 YYYY-MM-DD

# 运行爬虫程序
result_df = fetch_market_abnormal_stocks(TARGET_DATE)

# 在 Colab 中优美地展示表格结果
display(result_df)
