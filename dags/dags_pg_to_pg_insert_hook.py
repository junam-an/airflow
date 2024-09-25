
from airflow import DAG
import datetime
import pendulum

from airflow.operators.python import PythonOperator



###### DAG 설정 코드
with DAG(
    dag_id="dags_pg_to_pg_insert_hook",                           ## DAG 이름 8080 포트 콘솔화면에서 출력되는 이름, .py 파일 이름과 일치 시키기를 권장
    schedule="0 0 * * *",                                  ## crontab 스케쥴 정보, 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),    ## DAG 시작 시간 정보(언제 부터 수행 할 건지)
    catchup=False,                                         ## 시작시간과 현재 시간을 비교하여 누락 된 스케쥴 시간 만큼 수행 옵션, FALSE=사용안함 / TRUE=사용, TRUE시 누락분 만큼 병행 수행 됨 사용 하지 않길 권장
    #dagrun_timeout=datetime.timedelta(minutes=60),
    #tags=["example", "example2"],                         ## 이름 아래 작게 출력 되는 태그 이름
    #params={"example_key": "example_value"},              ## DAG에서 사용하는 값 파라미터
) as dag:
    def insrt_postgres(source_postgres_conn_id, target_postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        import pandas as pd

        source_postgres_hook = PostgresHook(source_postgres_conn_id)  # 소스 DB pg hook 설정
        with closing(source_postgres_hook.get_conn()) as source_conn: # 소스 DB 커넥션 수행
            with closing(source_conn.cursor()) as source_cursor: # 소스 DB 커서 생성
                print("hook select 수행")
                sql = 'select * from source_t;'
                source_cursor.execute(sql) # 소스 DB 쿼리 수행
                data = source_cursor.fetchall() # 쿼리 수행 출력 결과 가져오기
                columns = [col[0] for col in source_cursor.description]  # 컬럼 이름 추출
                df = pd.DataFrame(data, columns=columns) # 출력 데이터 데이터프레임으로 변환
        
        target_postgres_hook = PostgresHook(target_postgres_conn_id)  # 타겟 DB pg hook 설정
        with closing(target_postgres_hook.get_conn()) as target_conn: # 타겟 DB 커넥션 수행
            with closing(target_conn.cursor()) as target_cursor: # 타겟 DB 커서 생성
                    print("hook insert 수행")
                    for index, row in df.iterrows():             # 타겟 DB 에 데이터 insert
                        insert_query = f"""
                        INSERT INTO target_t ({', '.join(columns)}) 
                        VALUES ({', '.join(['%s'] * len(columns))})
                        """
                        target_cursor.execute(insert_query, tuple(row))

                    target_conn.commit()

                

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'source_postgres_conn_id':'conn-db-postgres-custom', 'target_postgres_conn_id':'conn-db-postgres-custom'} # Ariflow 콘솔에서 등록한 불러오려는 Connection ID 입력
    )