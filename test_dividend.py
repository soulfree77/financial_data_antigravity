import os
import FinanceDataReader as fdr
import OpenDartReader
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("DART_API_KEY")
dart = OpenDartReader(api_key)

corp_code = '00126380' # Samsung
year = '2023'

print("--- alot_matter ---")
try:
    dividend_info = dart.alot_matter(corp_code, year, '11011')
    print(dividend_info.head(2))
except Exception as e:
    print("Dividend Error:", e)

print("\n--- finstate_all for cash flow ---")
try:
    fs_all = dart.finstate_all(corp_code, year, fs_div='CFS')
    # search for cash flow
    mask = fs_all['account_nm'].str.contains('영업활동현금흐름|영업활동으로인한현금흐름', na=False)
    print(fs_all[mask][['sj_nm', 'account_nm', 'thstrm_amount']])
    
    # search for current assets / current liabilities
    mask2 = fs_all['account_nm'].str.contains('유동자산|유동부채', na=False)
    print(fs_all[mask2][['sj_nm', 'account_nm', 'thstrm_amount']].head(5))
except Exception as e:
    print("Finstate All Error:", e)
