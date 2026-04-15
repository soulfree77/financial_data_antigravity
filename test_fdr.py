import FinanceDataReader as fdr
df = fdr.StockListing('KRX-DESC')
print(df.columns)
print(df.head(2))
