import os
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from pprint import pprint

from airflow.operators.python import (
        PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
        )

with DAG(
        'kafka_evalu',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure' : False,
        'email_on_retry' : False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
        },
    max_active_tasks=3,
    max_active_runs=1,
    description='hello world DAG',
    #schedule=timedelta(days=1),
    schedule="0 9 * * *",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2023,1,2),
    catchup=True,
    tags=['notify', 'line'],
) as dag:

    def log(ds_nodash):
        import os
        from datetime import datetime, timedelta
        import logging
        def setup_logging(date):

            import os
            now_dir = os.getcwd()
            log_dir = os.path.join(now_dir, "logs", "cafe", "logs")
            
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            print(date)
            log_file = os.path.join(log_dir, f"log_{date}.log")
            
            if os.path.exists(log_file):
                os.remove(log_file)

            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(log_file),
                    logging.StreamHandler()
                ]
            )
            logging.info(f"Logging is set up. Logs will be saved in {log_file}")

        # 로그 설정 실행
        setup_logging(ds_nodash)

        # 외부 모듈 호출
        from lsiwh_simulate.customer import start
        start(logging,ds_nodash)
        
    agg_task = PythonVirtualenvOperator(
        task_id="agg.task",
        python_callable=log,
        requirements=["git+https://github.com/lsiwh37249/lsiwh_simulate.git"],
        system_site_packages=False,
    ) 
    
    log2csv = BashOperator(
        task_id="log2csv.task",
        bash_command="""
            $AIRFLOW_HOME/dags/log2csv.sh {{ds_nodash}}
        """ 
    )

    csv2parquet = BashOperator(
        task_id="csv2parquet.task",
        bash_command="""
           python /$AIRFLOW_HOME/dags/csv2parq.py {{ds_nodash}}
        """
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> agg_task >> log2csv >> csv2parquet >> task_end
