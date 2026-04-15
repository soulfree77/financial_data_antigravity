import os
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
if not api_key:
    print("DART_API_KEY is missing in .env")
    exit(1)

dart = OpenDartReader(api_key)

print("Fetching universe from KRX...")
krx = fdr.StockListing("KRX")
krx_desc = fdr.StockListing("KRX-DESC")

# Add Close price from krx
df = pd.merge(krx[['Code', 'Name', 'MarketId', 'Marcap', 'Close']], 
              krx_desc[['Code', 'Sector']], 
              on='Code', how='inner')

# KOSPI 200 (~ proxy top 200 STK), KOSDAQ 150 (~ proxy top 150 KSQ)
# Sorting by Marcap
kospi = df[df['MarketId'] == 'STK'].sort_values('Marcap', ascending=False).head(200)
kosdaq = df[df['MarketId'] == 'KSQ'].sort_values('Marcap', ascending=False).head(150)
universe = pd.concat([kospi, kosdaq])

# Exclude financial sectors
financial_keywords = ['금융', '은행', '보험', '증권', '투자', '저축', '지주']
mask = universe['Sector'].apply(lambda x: any(fw in str(x) for fw in financial_keywords) if pd.notnull(x) else False)
universe = universe[~mask].copy()

# Add corp_code from Dart for api mapping
corp_list = dart.corp_codes
stocks_in_dart = corp_list[corp_list['stock_code'].notnull()]
universe = pd.merge(universe, stocks_in_dart[['stock_code', 'corp_code']], left_on='Code', right_on='stock_code', how='inner')

def fetch_financials(row):
    corp_code = row['corp_code']
    code = row['Code']
    
    # Check 2025 Q3(11014), 2024 Year-end(11011), 2024 Q3(11014), 2023 Year-end(11011)
    # Using 2024 Year End as primary since 2025 might not be fully out
    reports_to_try = [('2025', '11011'), ('2025', '11014'), ('2024', '11011'), ('2024', '11014'), ('2023', '11011')]
    
    net_income = np.nan
    equity = np.nan
    liabilities = np.nan
    
    for year, reprt in reports_to_try:
        try:
            fs = dart.finstate(corp_code, year, reprt_code=reprt)
            time.sleep(0.1) # small delay to prevent limit
            if fs is not None and not fs.empty:
                fs_cfs = fs[fs['fs_div'] == 'CFS']
                if fs_cfs.empty:
                    fs_cfs = fs[fs['fs_div'] == 'OFS']
                
                if not fs_cfs.empty:
                    def get_amount(account_words):
                        for word in account_words:
                            mask = fs_cfs['account_nm'].str.contains(word, na=False, regex=False)
                            val = fs_cfs[mask]
                            if not val.empty:
                                amt = val.iloc[0]['thstrm_amount']
                                try:
                                    return int(str(amt).replace(',', ''))
                                except:
                                    pass
                        return np.nan
                        
                    equity = get_amount(['자본총계'])
                    liabilities = get_amount(['부채총계'])
                    net_income = get_amount(['당기순이익', '반기순이익', '분기순이익'])
                    
                    if pd.notnull(equity) and pd.notnull(liabilities) and pd.notnull(net_income):
                        break 
        except Exception as e:
            continue
            
    return {'Code': code, 'Equity': equity, 'Liabilities': liabilities, 'NetIncome': net_income}

print(f"Fetching financials for {len(universe)} companies. This might take a few minutes...")
results = []
# Using max_workers=5
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(fetch_financials, row): row for _, row in universe.iterrows()}
    for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
        res = future.result()
        results.append(res)
        
fin_df = pd.DataFrame(results)
data = pd.merge(universe, fin_df, on='Code', how='left')

# Calculate metrics
data['Close'] = pd.to_numeric(data['Close'], errors='coerce')
data['Marcap'] = pd.to_numeric(data['Marcap'], errors='coerce')
data['ROE'] = (data['NetIncome'] / data['Equity']) * 100
data['DebtRatio'] = (data['Liabilities'] / data['Equity']) * 100
data['PER'] = data['Marcap'] / data['NetIncome']
data['PBR'] = data['Marcap'] / data['Equity']

# Clean up negative/inf values for PER and PBR
data.loc[(data['PER'] <= 0) | np.isinf(data['PER']), 'PER'] = np.nan
data.loc[(data['PBR'] <= 0) | np.isinf(data['PBR']), 'PBR'] = np.nan

data = data.dropna(subset=['PER', 'PBR', 'ROE', 'DebtRatio'])
print(f"Total valid data after dropna: {len(data)}")
data.to_csv('debug_data.csv', index=False)

# Calculate Sector Averages
sector_stats = data.groupby('Sector').agg(
    Avg_ROE=('ROE', 'mean'),
    Avg_PER=('PER', 'mean'),
    PBR_30th=('PBR', lambda x: x.quantile(0.3))
).reset_index()

data = pd.merge(data, sector_stats, on='Sector', how='left')

# Filter logic
cond_roe = (data['ROE'] >= data['Avg_ROE']) | (data['ROE'] >= 10.0)
cond_debt = (data['DebtRatio'] < 100.0)
cond_per = (data['PER'] < data['Avg_PER'])
cond_pbr = (data['PBR'] < 1.0) | (data['PBR'] <= data['PBR_30th'])

print(f"cond_roe count: {cond_roe.sum()}")
print(f"cond_debt count: {cond_debt.sum()}")
print(f"cond_per count: {cond_per.sum()}")
print(f"cond_pbr count: {cond_pbr.sum()}")

screened = data[cond_roe & cond_debt & cond_per & cond_pbr].copy()
print(f"Final screened count: {len(screened)}")

# Score: (1/PER) + (1/PBR) + ROE
# Adjust scales so they don't overpower each other: using Z-scores would be much better
# but sticking to simple weights
# PER is usually 5-20, so 1/PER is 0.05 - 0.2
# PBR is 0.5-2, so 1/PBR is 0.5 - 2
# ROE is 5-20%
# We will do: (1/PER)*100 (range 5-20) + (1/PBR)*10 (range 5-20) + ROE (range 5-20)
screened['Score'] = (1 / screened['PER']) * 100 + (1 / screened['PBR']) * 10 + screened['ROE']

top10 = screened.sort_values(by='Score', ascending=False).head(10)

md = "## 🏆 수익성 & 저평가 우수 종목 Top 10 (KOSPI 200 / KOSDAQ 150 기준)\n\n"
md += "| 순위 | 종목명 (종목코드) | 현재가(원) & 시가총액(억원) | ROE (%) | 부채비율 (%) | PER / 업종평균 | PBR | 최종 스코어 |\n"
md += "|:---:|:---|---:|---:|---:|---:|---:|---:|\n"

for i, row in enumerate(top10.itertuples(), 1):
    name_code = f"**{row.Name}** (`{row.Code}`)"
    price_cap = f"{row.Close:,.0f}원 / {row.Marcap / 100000000:,.0f}억"
    roe = f"{row.ROE:.1f}%"
    dept = f"{row.DebtRatio:.1f}%"
    per_str = f"{row.PER:.1f} / {row.Avg_PER:.1f}"
    pbr = f"{row.PBR:.2f}"
    score = f"{row.Score:.1f}"
    
    md += f"| {i} | {name_code} | {price_cap} | {roe} | {dept} | {per_str} | {pbr} | {score} |\n"

print(md)

with open('screener_result.md', 'w') as f:
    f.write(md)

print("Finished evaluating.")
