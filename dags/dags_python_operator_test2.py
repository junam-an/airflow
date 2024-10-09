
from airflow import DAG
import datetime
import pendulum
import random

from airflow.operators.python import PythonOperator


def outer_func(target_func):
  def inner_func(**kwargs):
    from common.common_execute_pre import CustomPostgresHook
    log_table_write = CustomPostgresHook()

    dag_id = kwargs.get('ti').dag_id
    task_id = kwargs.get('ti').task_id
    execution_date = str(kwargs.get('execution_date'))
    run_id = str(kwargs.get('run_id'))
    state = kwargs.get('ti').state
    trigger_rule = kwargs.get('task').trigger_rule
    Operator = kwargs.get('ti').operator

    try:
      log_table_write.get_conn_pre(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='R', err_msg='', trigger_rule=trigger_rule, task_type=Operator)
    except Exception as e:
      log_table_write.get_conn_pre(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='E', err_msg=str(e)[0:4000], trigger_rule=trigger_rule, task_type=Operator)
      raise

    try:
      target_func()
    except Exception as e:
      log_table_write.get_conn_post(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='E', err_msg=str(e)[0:4000])
      raise

    try:
      log_table_write.get_conn_post(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='S', err_msg='')
    except Exception as e:
      log_table_write.get_conn_post(dag_id=dag_id, task_id=task_id, run_id=run_id, execute_id=execution_date, task_state='E', err_msg=str(e)[0:4000])
      raise
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
    from airflow.decorators import task
    @outer_func
    def select_fruit(**kwargs):
        ##### business logic 수행
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
        raise Exception("Intentional Failure")
        ##### business logic 종료


    py_t1 = PythonOperator(
        task_id='py_t1',               ## task name
        python_callable=select_fruit,  ## 실행 하고자 하는 파이썬 함수
        op_kwargs={},
        trigger_rule='all_done'        ## 선행 후행 task 실패 상관없이 실행
    )

    @outer_func
    def test_1(**kwargs):
      print('TEST JOB')

    py_t2 = PythonOperator(
        task_id='py_t2',               ## task name
        python_callable=test_1,  ## 실행 하고자 하는 파이썬 함수
        op_kwargs={},
        trigger_rule='all_done'
    )


    py_t1 >> py_t2
