
from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

###### DAG 설정 코드
with DAG(
    dag_id="dags_bash_operator",                           ## DAG 이름 8080 포트 콘솔화면에서 출력되는 이름, .py 파일 이름과 일치 시키기를 권장
    schedule="0 0 * * *",                                  ## crontab 스케쥴 정보, 분 시 일 월 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),    ## DAG 시작 시간 정보(언제 부터 수행 할 건지)
    catchup=False,                                         ## 시작시간과 현재 시간을 비교하여 누락 된 스케쥴 시간 만큼 수행 옵션, FALSE=사용안함 / TRUE=사용, TRUE시 누락분 만큼 병행 수행 됨 사용 하지 않길 권장
    #dagrun_timeout=datetime.timedelta(minutes=60),
    #tags=["example", "example2"],                         ## 이름 아래 작게 출력 되는 태그 이름
    #params={"example_key": "example_value"},              ## DAG에서 사용하는 값 파라미터
) as dag:
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",                          ## 콘솔화면 graph 탭 에서 출력되는 DAG task 이름, 객체 명과 동일 하게 권장
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",                          ## 콘솔화면 graph 탭 에서 출력되는 DAG task 이름, 객체 명과 동일 하게 권장
        bash_command="echo $HOSTNAME",
    )
    # [END howto_operator_bash]

    bash_t1 >> bash_t2                              ## bash_t1 수행 후 bash_t2 수행 하도록 설정