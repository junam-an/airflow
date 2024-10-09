from airflow.hooks.base import BaseHook
import psycopg2

class CustomPostgresHook(BaseHook):
    
    def __init__(self, **kwargs):
        self.postgres_conn_id = 'conn-db-postgres-custom'

    def get_conn_pre(self, dag_id, task_id, run_id, execute_id, task_state, err_msg):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.excute_id = execute_id
        self.task_state = task_state
        self.err_msg = err_msg

        self.log.info(self.postgres_conn_id)
        #self.log.info(self.host)
        #self.log.info(self.user)
        #self.log.info(self.password)
        #self.log.info(self.dbname)
        #self.log.info(self.port)
        #self.log.info(self.dag_id)
        #self.log.info(self.task_id)

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)

        self.log.info(f'log table task_log insert')
        if self.task_state == 'R':
            sql = "insert into airflow_task_log values (%s,%s, %s, %s, to_char(now(), 'YYYYMMDDHH24MISS'),NULL,%s,NULL,to_char(now(), 'YYYYMMDDHH24MISS'));"
        else:
            sql = "insert into airflow_task_log values (%s,%s, %s, %s, to_char(now(), 'YYYYMMDDHH24MISS'),to_char(now(), 'YYYYMMDDHH24MISS'),%s,%s,to_char(now(), 'YYYYMMDDHH24MISS'));"


        try:
            self.log.info(f'log table insert를 시작 합니다.')
            cursor = self.postgres_conn.cursor()
            if self.task_state == 'R':
                cursor.execute(sql,(self.dag_id, self.task_id, self.run_id, self.excute_id, self.task_state))
            else:
                cursor.execute(sql,(self.dag_id, self.task_id, self.run_id, self.excute_id, self.task_state, self.err_msg))
            self.postgres_conn.commit()
        except:
            self.log.info(f'log table insert 에 실패 하였습니다')
        
        return True
    

    def get_conn_post(self, dag_id, task_id, run_id, execute_id, task_state, err_msg):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.excute_id = execute_id
        self.task_state = task_state
        self.err_msg = err_msg

        self.log.info(self.postgres_conn_id)
        #self.log.info(self.host)
        #self.log.info(self.user)
        #self.log.info(self.password)
        #self.log.info(self.dbname)
        #self.log.info(self.port)
        #self.log.info(self.dag_id)
        #self.log.info(self.task_id)

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)

        self.log.info(f'log table task_log update')
        sql = "update airflow_task_log \
                set task_end_time = to_char(now(), 'YYYYMMDDHH24MISS') \
                    ,task_status = %s \
                    ,err_msg = %s \
                where 1=1 \
                and dag_name = %s \
                and task_name = %s \
                and run_id = %s \
                and execute_date = %s"

        try:
            self.log.info(f'log table update를 시작 합니다.')
            cursor = self.postgres_conn.cursor()
            if self.task_state == 'S':
                cursor.execute(sql,(self.task_state, 'complete', self.dag_id, self.task_id, self.run_id, self.excute_id))
            else:
                cursor.execute(sql,(self.task_state, self.err_msg, self.dag_id, self.task_id, self.run_id, self.excute_id))
            self.postgres_conn.commit()
        except:
            self.log.info(f'log table update에 실패 하였습니다')
        
        return True
        