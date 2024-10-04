from airflow.hooks.base import BaseHook
import psycopg2

class CustomPostgresHook(BaseHook):
    
    def __init__(self, **kwargs):
        self.postgres_conn_id = 'conn-db-postgres-custom'

    def get_conn(self, dag_id, task_id):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.log.info(self.postgres_conn_id)
        self.log.info(self.host)
        self.log.info(self.user)
        self.log.info(self.password)
        self.log.info(self.dbname)
        self.log.info(self.port)

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)

        self.log.info(f'log table data_interval_start insert')
        sql = "insert into airflow_task_log values ('test','test', to_char(now(), 'YYYYMMDDHH24MISS'),NULL,NULL,to_char(now(), 'YYYYMMDDHH24MISS'));"
        self.log.info(sql)


        try:
            self.log.info(f'insert를 시작 합니다.')
            cursor = self.postgres_conn.cursor()
            cursor.execute(sql)
            self.postgres_conn.commit()
        except:
            self.log.info(f'insert 에 실패 하였습니다')
        
        return True
        