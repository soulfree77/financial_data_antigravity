import os
from pykrx import stock
from pykrx.stock import get_index_ticker_list
import pandas as pd

# Check KOSPI and KOSDAQ indexes
kospi_indices = stock.get_index_ticker_list(market="KOSPI")
for idx in kospi_indices:
    name = stock.get_index_ticker_name(idx)
    if "200" in name:
        print(f"KOSPI: {idx} - {name}")

kosdaq_indices = stock.get_index_ticker_list(market="KOSDAQ")
for idx in kosdaq_indices:
    name = stock.get_index_ticker_name(idx)
    if "150" in name:
        print(f"KOSDAQ: {idx} - {name}")

date = "20260408" # recent market open date
df_per = stock.get_market_fundamental(date, market="ALL")
print(df_per.head())
