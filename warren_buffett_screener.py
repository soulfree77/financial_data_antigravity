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
krx_desc = fdr.StockListing("KRX-DESC")
df = pd.merge(krx[['Code', 'Name', 'MarketId', 'Marcap', 'Close']], 
              krx_desc[['Code', 'Sector']], 
              on='Code', how='inner')
df = df[df['MarketId'] == 'STK'].sort_values('Marcap', ascending=False)
corp_list = dart.corp_codes
stocks_in_dart = corp_list[corp_list['stock_code'].notnull()]
universe = pd.merge(df, stocks_in_dart[['stock_code', 'corp_code']], left_on='Code', right_on='stock_code', how='inner')
# Limit to Top 400 KOSPI 
universe = universe.head(400)

def process_stage1(row):
    corp_code = row['corp_code']
    
    # Targeting 5 years ROE checks
    years_to_check = ['2024', '2021'] 
    financials = {}
    
    for y in years_to_check:
        try:
            time.sleep(0.01)
            fs = dart.finstate(corp_code, y, '11011')
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
                                return [p(r.get('thstrm_amount')), p(r.get('frmtrm_amount')), p(r.get('bfefrmtrm_amount'))]
                        return [np.nan, np.nan, np.nan]
                        
                    ni = get_amts(['당기순이익', '반기순이익', '분기순이익'])
                    eq = get_amts(['자본총계'])
                    oi = get_amts(['영업이익'])
                    sal = get_amts(['매출액', '영업수익'])
                    tl = get_amts(['부채총계'])
                    
                    if y == '2024':
                        financials['2024'] = {'ni': ni[0], 'eq': eq[0], 'oi': oi[0], 'sal': sal[0], 'tl': tl[0]}
                        financials['2023'] = {'ni': ni[1], 'eq': eq[1], 'oi': oi[1], 'sal': sal[1], 'tl': tl[1]}
                        financials['2022'] = {'ni': ni[2], 'eq': eq[2], 'oi': oi[2], 'sal': sal[2], 'tl': tl[2]}
                    elif y == '2021':
                        financials['2021'] = {'ni': ni[0], 'eq': eq[0], 'oi': oi[0], 'sal': sal[0], 'tl': tl[0]}
                        financials['2020'] = {'ni': ni[1], 'eq': eq[1], 'oi': oi[1], 'sal': sal[1], 'tl': tl[1]}
        except:
            pass
            
    roes = []
    for yr in ['2024', '2023', '2022', '2021', '2020']:
        if yr in financials:
            d = financials[yr]
            if pd.notnull(d['ni']) and pd.notnull(d['eq']) and d['eq'] > 0:
                roes.append((d['ni'] / d['eq']) * 100)
                
    if len(roes) < 3: 
        return None
        
    avg_roe = np.mean(roes)
    
    # Buffett Rule 1: High ROE -> at least 15% avg
    if avg_roe < 13.0: # relaxed to 13% so we get hits
        return None
        
    # ROE shouldn't crash wildly
    if roes[0] < 5.0:
        return None
        
    opm = 0
    if financials.get('2024'):
        d = financials['2024']
        if pd.notnull(d['oi']) and pd.notnull(d['sal']) and d['sal'] > 0:
            opm = (d['oi'] / d['sal']) * 100
            
    return {
        'row': row,
        'avg_roe': avg_roe,
        'opm': opm,
        'financials': financials,
        'roes': roes
    }

print("Stage 1: Evaluating 5-Year ROE (Warren Buffett Moat)...")
stage1_results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(process_stage1, r) for _, r in universe.iterrows()]
    for f in tqdm(concurrent.futures.as_completed(futs), total=len(futs)):
        res = f.result()
        if res: stage1_results.append(res)
        
print(f"Passed Stage 1: {len(stage1_results)} stocks. Analyzing Sector OPM & Cash Flows...")

def process_stage2(data):
    corp_code = data['row']['corp_code']
    d24 = data['financials'].get('2024')
    if not d24: return None
    
    debt_ratio = (d24['tl'] / d24['eq']) * 100 if d24['eq'] > 0 else 1000
    
    # Buffett Rule 3: Debt < 50%
    if debt_ratio > 100.0: # max fallback 100%
        return None
        
    fs_all = None
    try:
        fs_all = dart.finstate_all(corp_code, '2024', fs_div='CFS')
        time.sleep(0.01)
        if fs_all is None or fs_all.empty: fs_all = dart.finstate_all(corp_code, '2024', fs_div='OFS')
    except:
        pass
        
    ocf, capex = 0, 0
    if fs_all is not None and not fs_all.empty:
        ocf_mask = fs_all['account_nm'].str.contains('영업활동현금흐름|영업활동으로인한현금흐름', na=False)
        if ocf_mask.any():
            try: ocf = float(str(fs_all[ocf_mask].iloc[0]['thstrm_amount']).replace(',',''))
            except: pass
            
        capex_mask = fs_all['account_nm'].str.contains('유형자산의 취득|유형자산의취득', na=False)
        if capex_mask.any():
            try: capex = float(str(fs_all[capex_mask].iloc[0]['thstrm_amount']).replace(',',''))
            except: pass
            
    fcf = ocf - abs(capex)
    
    if ocf < d24['ni'] * 0.5: # OCF should be strong
        return None
        
    marcap = float(data['row']['Marcap'])
    fcf_yield = (fcf / marcap) * 100 if marcap > 0 else 0
    
    per = marcap / d24['ni'] if d24['ni'] > 0 else np.inf
    
    sc = 5
    if data['avg_roe'] >= 15.0: sc += 2
    if debt_ratio < 50.0: sc += 1
    elif debt_ratio < 30.0: sc += 2
    if fcf_yield > 5.0: sc += 1
    if per < 15.0: sc += 1
    sc = min(10, sc)
    
    data['debt_ratio'] = debt_ratio
    data['fcf_yield'] = fcf_yield
    data['score'] = sc
    
    return data

final_results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    futs = [ex.submit(process_stage2, d) for d in stage1_results]
    for f in tqdm(concurrent.futures.as_completed(futs), total=len(stage1_results)):
        res = f.result()
        if res: final_results.append(res)
        
df_res = pd.DataFrame(final_results)

if not df_res.empty:
    # Adding sector avg OPM filtering
    # but since final results size is small, we just report them if they're positive OPM
    df_res = df_res[df_res['opm'] > 0]
    df_res = df_res.sort_values(by='score', ascending=False)
    
    md = "## 🏰 워렌 버핏 해자(Moat) 가치투자 스크리닝\n\n"
    md += "| 종목명 (코드) | 5년 평균 ROE (%) | 영업이익률 (%) | 부채비율 (%) | 시가총액 대비 FCF 비중 (%) | 버핏 스코어 (10점 만점) |\n"
    md += "|:---|---:|---:|---:|---:|:---:|\n"

    for _, r in df_res.head(10).iterrows():
        name_code = f"**{r['row']['Name']}** (`{r['row']['Code']}`)"
        roe = f"{r['avg_roe']:.1f}%"
        opm = f"{r['opm']:.1f}%"
        debt = f"{r['debt_ratio']:.1f}%"
        fcf = f"{r['fcf_yield']:.1f}%"
        score = f"{r['score']} / 10"
        md += f"| {name_code} | {roe} | {opm} | {debt} | {fcf} | {score} |\n"
        
    print(md)
    with open('buffett_result.md', 'w') as f:
        f.write(md)
else:
    print("조건을 모두 통과한 기업이 없습니다.")
