
from airflow import DAG
import datetime
import pendulum
import random

from airflow.operators.python import PythonOperator


def outer_func(target_func, **kwargs):
  def inner_func(**kwargs):
    print('target 함수 실행 전 입니다.')
    from common.common_execute_pre import CustomPostgresHook
    log_table_write = CustomPostgresHook()

    dag_id = kwargs.get('ti').dag_id
    task_id = kwargs.get('ti').task_id
    execution_date = str(kwargs.get('execution_date'))
    run_id = str(kwargs.get('run_id'))
    state = kwargs.get('ti').state

    log_table_write.get_conn_pre(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='R', err_msg='')

    target_func()
    print('target 함수 실행 후 입니다.')
  return inner_func


###### DAG 설정 코드
with DAG(
    dag_id="dags_python_operator_test2",                           ## DAG 이름 8080 포트 콘솔화면에서 출력되는 이름, .py 파일 이름과 일치 시키기를 권장
    schedule="0 0 * * *",                                  ## crontab 스케쥴 정보, 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),    ## DAG 시작 시간 정보(언제 부터 수행 할 건지)
    catchup=False,                                         ## 시작시간과 현재 시간을 비교하여 누락 된 스케쥴 시간 만큼 수행 옵션, FALSE=사용안함 / TRUE=사용, TRUE시 누락분 만큼 병행 수행 됨 사용 하지 않길 권장
    #dagrun_timeout=datetime.timedelta(minutes=60),
    #tags=["example", "example2"],                         ## 이름 아래 작게 출력 되는 태그 이름
    #params={"example_key": "example_value"},              ## DAG에서 사용하는 값 파라미터
) as dag:
    @outer_func
    def select_fruit(**kwargs):
        ##### business logic 수행
        try:
          fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
          rand_int = random.randint(0,3)
          print(fruit[rand_int])
        except Exception as e:
          raise
         ##### business logic 종료


    py_t1 = PythonOperator(
        task_id='py_t1',               ## task name
        python_callable=select_fruit,  ## 실행 하고자 하는 파이썬 함수
        op_kwargs={}
    )


    py_t1
