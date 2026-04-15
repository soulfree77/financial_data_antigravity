import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ.get("DART_API_KEY")

url = "https://opendart.fss.or.kr/api/alotMatter.json"
params = {
    'crtfc_key': api_key,
    'corp_code': '00126380',
    'bsns_year': '2023',
    'reprt_code': '11011'
}

response = requests.get(url, params=params).json()
if response.get('status') == '000':
    for item in response['list']:
        print(f"SE: {item.get('se')} | {item.get('thstrm')} | {item.get('frmtrm')} | {item.get('lwfr')}")
else:
    print(response)
