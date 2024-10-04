from airflow.hooks.base import BaseHook
import psycopg2

class CustomPostgresHook(BaseHook):
    
    def __init__(self, **kwargs):
        self.postgres_conn_id = 'conn-db-postgres-custom'

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        #self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        #return self.postgres_conn
    
        from contextlib import closing
        with closing(psycopg2.connect(host=self.host, dbname=self.dbname, user=self.user, password=self.password, port=int(self.port))) as conn:
            with closing(conn.cursor()) as conn:
                self.log.info(f'log table data_interval_start insert')
                sql = "insert into airflow_task_log values ('test','test', to_char(now(), 'YYYYMMDDHH24MISS'),NULL,NULL,to_char(now(), 'YYYYMMDDHH24MISS'));"

                try:
                    conn.execute(sql)
                    conn.commit()
                except:
                    self.log.info(f'insert 에 실패 하였습니다')
        