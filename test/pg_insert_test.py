
def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing
        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                msg = 'insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'

            #try:
                cursor.execute(sql,('100','200','300',msg))
                conn.commit()
            #except Exception as e:
             #   print(e.message)



insrt_postgres('localhost', '5432', 'ajnam', 'ajnam', 'ajnam')