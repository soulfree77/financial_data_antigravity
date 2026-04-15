from pykrx import stock
try:
    df = stock.get_market_fundamental("20260408", market="ALL")
    print(df.head())
except Exception as e:
    print("Fundamental API Error:", e)

import FinanceDataReader as fdr
df_fdr = fdr.StockListing('KRX')
print(df_fdr.head())
