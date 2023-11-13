import ray
from util.db.pg.engine import create_postgresql_engine_by_adbc

@ray.remote
def load_postgresql_data(user: str, pwd: str, ip: str, port: str | int, db: str,schema_name:str,table_name: str):

    pg_conn = create_postgresql_engine_by_adbc(user=user, pwd=pwd, ip=ip, port=port, db=db)
    with pg_conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {schema_name}.{table_name}")
        pyarrow_table = cur.fetch_arrow_table()
    pg_conn.close()
    return pyarrow_table



