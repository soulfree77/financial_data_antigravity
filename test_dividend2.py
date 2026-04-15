import os
import FinanceDataReader as fdr
import OpenDartReader
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("DART_API_KEY")
dart = OpenDartReader(api_key)

try:
    df = dart.report('alotMatter', '00126380', '2023', '11011')
    print(df[['se', 'thstrm', 'frmtrm', 'lwfr']])
except Exception as e:
    print("Error:", e)
