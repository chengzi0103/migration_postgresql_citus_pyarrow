import pandas as pd
from dbutil.postgres.engine import create_postgresql_engine_by_adbc



def load_data_from_postgresql_by_adbc_cursor(cursor,sql:str):
    cursor.execute(sql)
    return cursor.fetch_arrow_table()

def load_postgresql_citus_node_ip(conn):
    return pd.read_sql(sql='SELECT groupid,nodename FROM pg_dist_node where isactive=true',con=conn)


def load_postgresql_data_use_adbc_to_pyarrow_table(conn,sql:str)->pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        pyarrow_table = cur.fetch_arrow_table()
    return pyarrow_table


def load_postgresql_data_to_pyarrow_table(user: str, pwd: str, ip: str, port: str | int, db: str,
                                          shard_names: list) -> pd.DataFrame:
    pg_conn = create_postgresql_engine_by_adbc(user=user, pwd=pwd, ip=ip, port=port, db=db)
    data = {}
    with pg_conn.cursor() as cur:
        for shard_name in shard_names:
            cur.execute(f"SELECT * FROM {shard_name}")
            data[shard_name] = cur.fetch_arrow_table()
    pg_conn.close()
    return data
