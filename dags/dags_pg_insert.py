
from airflow import DAG
import datetime
import pendulum

from airflow.operators.python import PythonOperator


###### DAG 설정 코드
with DAG(
    dag_id="dags_pg_insert",                           ## DAG 이름 8080 포트 콘솔화면에서 출력되는 이름, .py 파일 이름과 일치 시키기를 권장
    schedule="0 0 * * *",                                  ## crontab 스케쥴 정보, 분 시 일 월 요일
    start_date=pendulum.datetime(2024, 12, 1, tz="Asia/Seoul"),    ## DAG 시작 시간 정보(언제 부터 수행 할 건지)
    catchup=False,                                         ## 시작시간과 현재 시간을 비교하여 누락 된 스케쥴 시간 만큼 수행 옵션, FALSE=사용안함 / TRUE=사용, TRUE시 누락분 만큼 병행 수행 됨 사용 하지 않길 권장
    #dagrun_timeout=datetime.timedelta(minutes=60),
    #tags=["example", "example2"],                         ## 이름 아래 작게 출력 되는 태그 이름
    #params={"example_key": "example_value"},              ## DAG에서 사용하는 값 파라미터
) as dag:
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'

            try:
                cursor.execute(sql,(dag_id,task_id,run_id,msg))
                conn.commit()
            except Exception as e:
                return str(e.message)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['172.28.0.3', '5432', 'ajnam', 'ajnam', 'ajnam']
    )

    insrt_postgres