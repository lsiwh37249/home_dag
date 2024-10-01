#!/bin/bash

# 로그 파일 경로
LOG_FILE="/home/kim/airflow/logs/cafe/logs/log_$1.log"
# CSV 파일 경로
mkdir -p "/home/kim/airflow/logs/cafe/csv"

CSV_FILE="/home/kim/airflow/logs/cafe/csv/log_$1.csv"

if [ -f "$CSV_FILE" ]; then
    rm "$CSV_FILE"
    echo "파일이 삭제되었습니다: $CSV_FILE"
fi
# CSV 파일의 헤더 작성
echo "server Date,Time,LogLevel,CustomerID,customerDate,Message" > $CSV_FILE

# 로그 파일을 읽고 각 줄을 CSV 형식으로 변환
while read -r line; do
    # 날짜 및 시간, 로그 레벨 추출
    log_date=$(echo "$line" | awk '{print $1}')
    log_time=$(echo "$line" | awk '{print $2}' | cut -d',' -f1)
    log_level=$(echo "$line" | awk -F' - ' '{print $2}')

    # 고객 ID 및 주문 날짜 추출
    customer_id=$(echo "$line" | awk -F' - ' '{print $3}' | cut -d',' -f1 | xargs)
    order_date=$(echo "$line" | grep -oP '\[\K[^\]]+')

    # 메시지 추출
    message=$(echo "$line" | awk -F', ' '{print $3}' | xargs)

    # CSV 형식으로 변환하여 출력
    echo "$log_date,$log_time,$log_level,$customer_id,$order_date,\"$message\"" >> $CSV_FILE
done < "$LOG_FILE"

echo "CSV 파일 생성 완료: $CSV_FILE"
