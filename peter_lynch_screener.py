import os
import time
import requests
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
universe = pd.merge(df, stocks_in_dart[['stock_code', 'corp_code']], left_on='Code', right_on='stock_code', how='inner')
universe = universe.head(400) # Extended to 400 to get enough Peter Lynch candidates

def process_stage1(row):
    corp_code = row['corp_code']
    code = row['Code']
    price = pd.to_numeric(row['Close'], errors='coerce')
    marcap = pd.to_numeric(row['Marcap'], errors='coerce')
    
    url = "https://opendart.fss.or.kr/api/alotMatter.json"
    
    eps_latest, eps_prev, eps_old = 0, 0, 0
    passed_eps = False
    
    for year in ['2024', '2023']:
        params = {'crtfc_key': api_key, 'corp_code': corp_code, 'bsns_year': year, 'reprt_code': '11011'}
        time.sleep(0.01)
        try:
            res = requests.get(url, params=params).json()
            if res.get('status') == '000':
                items = res['list']
                eps_items = [x for x in items if '주당순이익' in x['se'] and '연결' in x['se']]
                if not eps_items:
                    eps_items = [x for x in items if '주당순이익' in x['se']]
                if not eps_items: continue
                
                target_item = eps_items[0]
                
                def parse_amt(val):
                    if not val or val == '-': return 0.0
                    try: return float(str(val).replace(',', ''))
                    except: return 0.0
                    
                eps_latest = parse_amt(target_item.get('thstrm'))
                eps_prev = parse_amt(target_item.get('frmtrm'))
                eps_old = parse_amt(target_item.get('lwfr'))
                passed_eps = True
                break
        except:
            pass
            
    if not passed_eps or eps_old <= 0 or eps_latest <= 0:
        return None
        
    cagr = ((eps_latest / eps_old) ** 0.5 - 1) * 100
    
    # Peter Lynch Filter 1: EPS CAGR between 10% and 30%
    if not (10.0 <= cagr < 30.0):
        return None
        
    per = price / eps_latest if eps_latest > 0 else np.inf
    peg = per / cagr if cagr > 0 else np.inf
    
    # Peter Lynch Filter 2: PEG Ratio <= 1.0
    if peg > 1.0 or peg <= 0:
        return None
        
    return {
        'row': row,
        'eps_cagr': cagr,
        'per': per,
        'peg': peg,
        'eps_latest': eps_latest
    }

print("Stage 1: Evaluating EPS Growth and PEG (fast filter)...")
stage1_results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(process_stage1, r) for _, r in universe.iterrows()]
    for f in tqdm(concurrent.futures.as_completed(futs), total=len(futs)):
        res = f.result()
        if res: stage1_results.append(res)
        
print(f"Passed Stage 1: {len(stage1_results)} stocks. Proceeding to fundamental analysis...")

def process_stage2(data):
    corp_code = data['row']['corp_code']
    
    debt_ok = False
    inv_ok = False
    dr_t = np.nan
    net_cash_ratio = np.nan
    
    # checking over recent available reports
    for year in ['2024', '2023']:
        try:
            fs = dart.finstate(corp_code, year, '11011')
            time.sleep(0.05)
            if fs is not None and not fs.empty:
                fs_cfs = fs[fs['fs_div'] == 'CFS']
                if fs_cfs.empty: fs_cfs = fs[fs['fs_div'] == 'OFS']
                if not fs_cfs.empty:
                    def get_amts(words):
                        for w in words:
                            m = fs_cfs['account_nm'].str.contains(w, na=False, regex=False)
                            if m.any():
                                r = fs_cfs[m].iloc[0]
                                def p(v):
                                    if pd.isna(v) or v=='-' or not v: return 0.0
                                    try: return float(str(v).replace(',',''))
                                    except: return 0.0
                                return p(r.get('thstrm_amount')), p(r.get('frmtrm_amount')), p(r.get('bfefrmtrm_amount'))
                        return 0,0,0
                        
                    tl_t, tl_t1, tl_t2 = get_amts(['부채총계'])
                    te_t, te_t1, te_t2 = get_amts(['자본총계'])
                    s_t, s_t1, s_t2 = get_amts(['매출액', '영업수익'])
                    
                    if te_t > 0:
                        dr_t = (tl_t/te_t)*100
                        dr_t1 = (tl_t1/te_t1)*100 if te_t1 > 0 else 1000
                        dr_t2 = (tl_t2/te_t2)*100 if te_t2 > 0 else 1000
                        # Peter Lynch Filter 3: Debt <= 100% or trending down
                        if dr_t <= 100.0 or (dr_t < dr_t1 and dr_t1 < dr_t2):
                            debt_ok = True
                            
                    if debt_ok:
                        sales_grow_t = s_t / s_t1 - 1 if s_t1 > 0 else 0
                        
                        # Peter Lynch Filter 4 (Inventory) & Filter 5 (Net Cash) via finstate_all
                        fs_all = dart.finstate_all(corp_code, year, fs_div='CFS')
                        if fs_all is None or fs_all.empty: fs_all = dart.finstate_all(corp_code, year, fs_div='OFS')
                        if fs_all is not None and not fs_all.empty:
                            def get_all_amts(words):
                                for w in words:
                                    m = fs_all['account_nm'].str.contains(w, na=False)
                                    if m.any():
                                        r = fs_all[m].iloc[0]
                                        def p(v):
                                            if pd.isna(v) or v=='-' or not v: return 0.0
                                            try: return float(str(v).replace(',',''))
                                            except: return 0.0
                                        return p(r.get('thstrm_amount')), p(r.get('frmtrm_amount'))
                                return 0,0
                                
                            cash_t, _ = get_all_amts(['현금및현금성자산', '현금 및 현금성자산', '현금 및 현금성 자산'])
                            inv_t, inv_t1 = get_all_amts(['재고자산'])
                            
                            inv_grow_t = inv_t / inv_t1 - 1 if inv_t1 > 0 else 0
                            
                            # if inventory grows slower than sales, or no inventory (like services), pass
                            if inv_grow_t <= sales_grow_t or inv_t == 0:
                                inv_ok = True
                                
                            net_cash = cash_t - tl_t
                            marcap = float(data['row']['Marcap'])
                            net_cash_ratio = (net_cash / marcap) * 100 if marcap > 0 else 0
                            
            if debt_ok and inv_ok:
                break
        except:
            pass

    if debt_ok and inv_ok:
        sc = 6 # baseline for passing all strict tests
        if data['peg'] <= 0.5: sc += 2
        elif data['peg'] <= 0.8: sc += 1
        if net_cash_ratio > 0: sc += 2 # positive net cash is huge
        if dr_t <= 50.0: sc += 1
        sc = min(10, sc) # cap at 10
        
        return {
            'Name': data['row']['Name'],
            'Code': data['row']['Code'],
            'CAGR': data['eps_cagr'],
            'PER': data['per'],
            'PEG': data['peg'],
            'DebtRatio': dr_t,
            'NetCashRatio': net_cash_ratio,
            'Score': sc
        }
    return None

print("Stage 2: Analyzing statements for Debt, Net Cash and Inventory...")
final_results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(process_stage2, d) for d in stage1_results]
    for f in tqdm(concurrent.futures.as_completed(futs), total=len(futs)):
        res = f.result()
        if res: final_results.append(res)
        
if not final_results:
    print("조건을 모두 통과한 종목이 하나도 없습니다!")
    # empty markdown
else:
    df_res = pd.DataFrame(final_results)
    df_res = df_res.sort_values(by='Score', ascending=False)
    
    md = "## 🏆 피터 린치 가치투자 스크리닝 (GARP 전략)\n\n"
    md += "| 종목명 (코드) | 3년 연평균 EPS 성장률 | PER / PEG | 부채비율 | 순현금 비중 (시총 대비) | 최종 린치 점수 |\n"
    md += "|:---|---:|---:|---:|---:|:---:|\n"

    for _, r in df_res.iterrows():
        name_code = f"**{r['Name']}** (`{r['Code']}`)"
        cagr = f"{r['CAGR']:.1f}%"
        per_peg = f"{r['PER']:.1f}배 / **{r['PEG']:.2f}**"
        debt = f"{r['DebtRatio']:.1f}%"
        ncr = f"{r['NetCashRatio']:.1f}%"
        score = f"{r['Score']} / 10"
        md += f"| {name_code} | {cagr} | {per_peg} | {debt} | {ncr} | {score} |\n"
        
    print(md)
    with open('lynch_result.md', 'w') as f:
        f.write(md)

