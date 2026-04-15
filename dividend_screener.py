import os
import requests
import time
import concurrent.futures
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import OpenDartReader
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()
api_key = os.environ.get("DART_API_KEY")
dart = OpenDartReader(api_key)

print("Fetching KOSPI universe from KRX...")
krx = fdr.StockListing("KRX")
df = krx[krx['MarketId'] == 'STK'].sort_values('Marcap', ascending=False)

corp_list = dart.corp_codes
stocks_in_dart = corp_list[corp_list['stock_code'].notnull()]
# We'll analyze the top 300 KOSPI components for reliable processing time and investable size.
universe = pd.merge(df, stocks_in_dart[['stock_code', 'corp_code']], left_on='Code', right_on='stock_code', how='inner')
universe = universe.head(300)

def process_stock(row):
    corp_code = row['corp_code']
    code = row['Code']
    price = pd.to_numeric(row['Close'], errors='coerce')
    
    url = "https://opendart.fss.or.kr/api/alotMatter.json"
    
    dividend_pass = False
    dps_latest, dps_prev, dps_old = 0, 0, 0
    payout_ratio = 0
    yield_3yr_avg = 0
    
    years = ['2023', '2024'] # start with 2023 for full 3-year data if 2024 is missing
    # Actually checking 2024 first
    for year in ['2024', '2023']:
        params = {'crtfc_key': api_key, 'corp_code': corp_code, 'bsns_year': year, 'reprt_code': '11011'}
        time.sleep(0.05)
        try:
            res = requests.get(url, params=params).json()
            if res.get('status') == '000':
                items = res['list']
                
                dps_items = [x for x in items if x['se'] == '주당 현금배당금(원)']
                if not dps_items: continue
                dps_row = dps_items[0]
                
                def parse_amt(val):
                    if not val or val == '-': return 0.0
                    try: return float(str(val).replace(',', ''))
                    except: return 0.0
                    
                dps_latest = parse_amt(dps_row.get('thstrm'))
                dps_prev = parse_amt(dps_row.get('frmtrm'))
                dps_old = parse_amt(dps_row.get('lwfr'))
                
                pr_items = [x for x in items if x['se'] == '(연결)현금배당성향(%)' or x['se'] == '(별도)현금배당성향(%)']
                if pr_items:
                    payout_ratio = parse_amt(pr_items[0].get('thstrm'))
                
                yield_items = [x for x in items if x['se'] == '현금배당수익률(%)']
                if yield_items:
                    y_latest = parse_amt(yield_items[0].get('thstrm'))
                    y_prev = parse_amt(yield_items[0].get('frmtrm'))
                    y_old = parse_amt(yield_items[0].get('lwfr'))
                    yield_3yr_avg = (y_latest + y_prev + y_old) / 3.0
                
                break 
        except:
            pass

    if dps_latest == 0:
        return None 
        
    current_yield = (dps_latest / price) * 100 if price > 0 else 0
    
    # Filter 1: Yield
    if not (current_yield > yield_3yr_avg or current_yield >= 4.0):
        return None
        
    # Filter 2: Stability
    if not (dps_latest >= dps_prev and dps_prev >= dps_old):
        return None
        
    # Filter 3: Payout ratio
    if not (20.0 <= payout_ratio < 60.0):
        return None
        
    cf_ok = False
    finance_ok = False
    pbr = np.nan
    debt_ratio = 100.0
    cf_net_ratio = 0.0
    
    # Need to check 2024 or 2023 for financials
    for year in ['2025', '2024', '2023']:
        reprt = '11011'
        try:
            fs = dart.finstate(corp_code, year, reprt)
            time.sleep(0.05)
            if fs is not None and not fs.empty:
                fs_cfs = fs[fs['fs_div'] == 'CFS']
                if fs_cfs.empty: fs_cfs = fs[fs['fs_div'] == 'OFS']
                
                if not fs_cfs.empty:
                    def get_amt(words):
                        for w in words:
                            m = fs_cfs['account_nm'].str.contains(w, na=False, regex=False)
                            if m.any(): return float(str(fs_cfs[m].iloc[0]['thstrm_amount']).replace(',',''))
                        return np.nan
                    
                    current_assets = get_amt(['유동자산'])
                    current_liab = get_amt(['유동부채'])
                    total_liab = get_amt(['부채총계'])
                    total_equity = get_amt(['자본총계'])
                    net_income = get_amt(['당기순이익', '반기순이익', '분기순이익'])
                    
                    if pd.notnull(total_liab) and pd.notnull(total_equity) and total_equity > 0:
                        debt_ratio = (total_liab / total_equity) * 100
                        finance_ok = debt_ratio <= 150.0
                    else: finance_ok = False
                    
                    if pd.notnull(current_assets) and pd.notnull(current_liab) and current_liab > 0:
                        curr_ratio = (current_assets / current_liab) * 100
                        finance_ok = finance_ok and (curr_ratio >= 100.0)
                    else: finance_ok = False
                    
                    if finance_ok and pd.notnull(net_income):
                        marcap = float(row['Marcap'])
                        pbr = marcap / total_equity
                        
                        # cash flow
                        fs_all = dart.finstate_all(corp_code, year, fs_div='CFS')
                        if fs_all is None or fs_all.empty:
                            fs_all = dart.finstate_all(corp_code, year, fs_div='OFS')
                            
                        if fs_all is not None and not fs_all.empty:
                            cf_mask = fs_all['account_nm'].str.contains('영업활동현금흐름|영업활동으로인한현금흐름', na=False)
                            if cf_mask.any():
                                cf = float(str(fs_all[cf_mask].iloc[0]['thstrm_amount']).replace(',',''))
                                cf_ok = (cf > net_income) and (cf > 0)
                                if net_income > 0: cf_net_ratio = cf / net_income
                                
                    if finance_ok and cf_ok:
                        break 
        except:
            pass
            
    if finance_ok and cf_ok:
        # 3 year compound growth rate assuming dps_old is 2 years ago
        grow_rate = (((dps_latest / dps_old) ** 0.5) - 1) * 100 if dps_old > 0 else 0
        return {
            'Name': row['Name'],
            'Code': row['Code'],
            'CurrYield': current_yield,
            'GrowRate': grow_rate,
            'PayoutRatio': payout_ratio,
            'PBR': pbr,
            'DebtRatio': debt_ratio,
            'CF_NetIncome_Ratio': cf_net_ratio
        }
    return None

results = []
print("Evaluating KOSPI Dividend Aristocrats...")
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(process_stock, r) for _, r in universe.iterrows()]
    for f in tqdm(concurrent.futures.as_completed(futs), total=len(futs)):
        res = f.result()
        if res: results.append(res)
        
if not results:
    print("조건을 모두 통과한 종목이 하나도 없습니다!")
    # If too strict, we will dump an empty markdown.
    df_res = pd.DataFrame()
else:
    df_res = pd.DataFrame(results)
    
    # Calculate score
    # Score = CurrYield + (100 / PayoutRatio) + GrowRate
    df_res['Score'] = df_res['CurrYield'] + (100.0 / df_res['PayoutRatio']) + df_res['GrowRate']
    
    df_res = df_res.sort_values(by='Score', ascending=False)
    
    # Sustainability score calculation (out of 5)
    def calc_sustain(row):
        pts = 0
        if row['CurrYield'] >= 5.0: pts += 1
        if row['PayoutRatio'] <= 40.0: pts += 1
        if row['GrowRate'] >= 10.0: pts += 1
        if row['DebtRatio'] <= 70.0: pts += 1
        if row['CF_NetIncome_Ratio'] >= 1.5: pts += 1
        # baseline: always at least gives 1 since it passed strict rules
        return max(1, pts)
        
    df_res['SustainScore'] = df_res.apply(calc_sustain, axis=1)
    
    top10 = df_res.head(10)
    
    md = "## 📈 고배당 & 성장 KOSPI 우수 종목 Top 10\n\n"
    md += "| 종목명 (코드) | 현재 시가배당률 (%) | 3년 평균 배당성장률 (%) | 배당성향 (%) | PBR (배) | 지속 가능성 점수 (5점 만점) |\n"
    md += "|:---|---:|---:|---:|---:|:---:|\n"
    
    def render_stars(score):
        return "⭐" * int(score) + "☆" * (5 - int(score))

    for _, r in top10.iterrows():
        name_code = f"**{r['Name']}** (`{r['Code']}`)"
        y = f"{r['CurrYield']:.2f}%"
        g = f"{r['GrowRate']:.2f}%"
        pr = f"{r['PayoutRatio']:.1f}%"
        pbr = f"{r['PBR']:.2f}"
        stars = render_stars(r['SustainScore'])
        md += f"| {name_code} | {y} | {g} | {pr} | {pbr} | {stars} ({r['SustainScore']}점) |\n"
        
    print(md)
    with open('dividend_result.md', 'w') as f:
        f.write(md)

