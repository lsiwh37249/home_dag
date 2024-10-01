import pandas as pd
import sys
import os

ds_nodash = sys.argv[1]

print(f"ds_nodash : {ds_nodash}")
# CSV 파일 경로
csv_file_path = '/home/kim/airflow/logs/cafe/csv/log_20230101.csv'
# 출력할 Parquet 파일 경로

parquet_dir_path = f"/home/kim/airflow/logs/cafe/parquet"

if not os.path.exists(parquet_dir_path):
    # 디렉터리가 없으면 생성
    os.mkdir(parquet_dir_path)

# CSV 파일을 읽어 DataFrame으로 변환
df = pd.read_csv(csv_file_path)


parquet_file_path = f"/home/kim/airflow/logs/cafe/parquet/{ds_nodash}"

# 파일 존재 여부 확인
#if not os.path.exists(parquet_file_path):
#    # 파일이 없으면 파일을 생성 (쓰기 모드)
#    with open(parquet_file_path, 'w') as file:
#        file.write("===================")
#    print(f"파일이 생성되었습니다: {file_path}")

# DataFrame을 Parquet 파일로 저장
df.to_parquet(parquet_file_path, index=False)

print(f"CSV 파일 '{csv_file_path}'가 Parquet 파일 '{parquet_file_path}'로 변환되었습니다.")

# Parquet 파일을 읽어 DataFrame으로 변환
df_parquet = pd.read_parquet(parquet_file_path)

# Parquet 파일 내용 출력
print("Parquet 파일 내용:")
print(df_parquet)
