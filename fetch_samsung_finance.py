import os
import requests
import pandas as pd
from dotenv import load_dotenv

# .env 파일에서 환경변수 로드
load_dotenv()
DART_API_KEY = os.environ.get("DART_API_KEY")

if not DART_API_KEY or DART_API_KEY == "여기에_API_키를_입력하세요":
    print("경고: DART API Key가 설정되지 않았습니다. .env 파일에 API Key를 입력해주세요.")
    exit(1)

# 삼성전자 고유코드
CORP_CODE = "00126380"
# 사업보고서 코드
REPRT_CODE = "11011"
# 최근 10년도 설정 (2014 ~ 2023)
YEARS = list(range(2014, 2024))

URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"

all_data = []

print("데이터 수집을 시작합니다...")

for year in YEARS:
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": CORP_CODE,
        "bsns_year": str(year),
        "reprt_code": REPRT_CODE
    }
    
    response = requests.get(URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "000":
            print(f"{year}년도 데이터 수집 성공")
            list_data = data.get("list", [])
            for item in list_data:
                # 연결재무제표(CFS) 기준
                if item.get("fs_div") == "CFS":
                    all_data.append({
                        "연도": year,
                        "계정과목": item.get("account_nm"),
                        "금액": item.get("thstrm_amount")
                    })
        elif data.get("status") == "013":
            print(f"{year}년도 데이터 없음: 공시 정보가 없을 수 있습니다.")
        else:
            print(f"{year}년도 수집 실패: {data.get('message', data.get('status'))}")
    else:
        print(f"{year}년도 API 호출 실패 (HTTP Status: {response.status_code})")

if not all_data:
    print("수집된 데이터가 없으므로 종료합니다.")
    exit()

df = pd.DataFrame(all_data)

# '금액' 문자열 정리 후 숫자(int) 변환
def parse_amount(val):
    if pd.isna(val) or val == '' or val == '-':
        return 0
    try:
        return int(str(val).replace(',', ''))
    except ValueError:
        return val

df['금액'] = df['금액'].apply(parse_amount)

# 중복 제거 (혹시 같은 계정과목이 여러 개 넘어오는 경우 대비해 첫 번째 값 사용)
df = df.drop_duplicates(subset=["연도", "계정과목"], keep="first")

# Pivot (행: 계정과목, 열: 연도)
pivot_df = df.pivot(index="계정과목", columns="연도", values="금액")

# 단위 변환 (억 원)
pivot_df = pivot_df / 100000000

# 엑셀 파일로 저장
output_filename = "samsung_electronics_finance.xlsx"
pivot_df.to_excel(output_filename)

print(f"\n데이터가 '{output_filename}' 파일에 성공적으로 저장되었습니다.")
print("단위: 억 원\n")

# 주요 지표를 마크다운 형태의 표로 출력하기 위해 데이터프레임 필터링
important_accounts = [
    "유동자산", "비유동자산", "자산총계", 
    "유동부채", "비유동부채", "부채총계", 
    "자본금", "이익잉여금", "자본총계", 
    "매출액", "영업이익", "법인세차감전순이익", "당기순이익"
]

available_accounts = [acc for acc in important_accounts if acc in pivot_df.index]

if available_accounts:
    summary_df = pivot_df.loc[available_accounts]
else:
    summary_df = pivot_df

# 출력
markdown_table = summary_df.to_markdown(floatfmt=".0f")
print("\n=== 삼성전자 주요 재무정보 요약 (단위: 억 원) ===")
print("이 결과는 아티팩트를 위해 복사됩니다.")
print(markdown_table)
