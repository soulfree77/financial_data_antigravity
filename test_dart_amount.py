import os, FinanceDataReader as fdr, OpenDartReader
from dotenv import load_dotenv
load_dotenv()
api_key = os.environ.get("DART_API_KEY")
dart = OpenDartReader(api_key)
# Samsung Electronics
print(dart.finstate('00126380', '2023', '11011')[['account_nm', 'thstrm_amount']])
