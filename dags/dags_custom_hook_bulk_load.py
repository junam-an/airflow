
from airflow import DAG
import pendulum

from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgresHook

###### DAG 설정 코드
with DAG(
    dag_id="dags_pg_insert_hook",                           ## DAG 이름 8080 포트 콘솔화면에서 출력되는 이름, .py 파일 이름과 일치 시키기를 권장
    schedule="0 0 * * *",                                  ## crontab 스케쥴 정보, 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),    ## DAG 시작 시간 정보(언제 부터 수행 할 건지)
    catchup=False,                                         ## 시작시간과 현재 시간을 비교하여 누락 된 스케쥴 시간 만큼 수행 옵션, FALSE=사용안함 / TRUE=사용, TRUE시 누락분 만큼 병행 수행 됨 사용 하지 않길 권장
    #dagrun_timeout=datetime.timedelta(minutes=60),
    #tags=["example", "example2"],                         ## 이름 아래 작게 출력 되는 태그 이름
    #params={"example_key": "example_value"},              ## DAG에서 사용하는 값 파라미터
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        CustomPostgresHook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        CustomPostgresHook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCorona19Countstatus_bulk2',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19Countstatus_bulk2.csv'}
    )
