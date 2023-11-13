import pandas as pd


def load_postgresql_distribute_table_task(pg_conn):
    distribute_job_task = []
    for node_ip in pd.read_sql(sql='SELECT groupid,nodename FROM pg_dist_node where isactive=true',con=pg_conn):
        shards_name_df = pd.read_sql(
            sql=f"select table_name as full_name,shard_name,shard_size from citus_shards where nodename='{node_ip}' and citus_table_type='distributed' and shard_size !=0 ",
            con=pg_conn)
        shards_name_df['ip'] = node_ip
        distribute_job_task.append(shards_name_df)
    distribute_job_task:pd.DataFrame = pd.concat(distribute_job_task) # get_distribute_table
    distribute_job_task['schema_name'] = distribute_job_task['full_name'].apply(lambda x: x.split('.')[0])
    distribute_job_task['table_name'] = distribute_job_task['full_name'].apply(lambda x: x.split('.')[1])
    return distribute_job_task



def load_postgresql_reference_table_task(pg_conn):
    reference_job_task = pd.read_sql(sql="select table_name as full_name from citus_shards where  citus_table_type='reference'  group by table_name",con=pg_conn)
    reference_job_task['schema_name'] = reference_job_task['full_name'].apply(lambda x: x.split('.')[0])
    reference_job_task['table_name'] = reference_job_task['full_name'].apply(lambda x: x.split('.')[1])
    return reference_job_task


