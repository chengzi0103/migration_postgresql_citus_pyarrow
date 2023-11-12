import traceback
from typing import Literal
import pandas as pd
from pgpq import ArrowToPostgresBinaryEncoder
from pyarrow import dataset as ds



def dump_dataframe_to_postgres_without_orm(conn, df: pd.DataFrame, schema_name: str, table_name: str,
                                           if_exists: Literal["fail", "replace", "append"] = 'append', index_type=False,
                                           chunksize=20000):
    try:
        df.to_sql(table_name, con=conn, index=index_type, if_exists=if_exists, schema=schema_name,
                  chunksize=chunksize)
    except Exception as e:
        ero = traceback.print_exc()
        print(ero)
        raise Exception(ero)



def dump_pyarrow_dataset_to_postgresql_use_pgpq(conn, schema_name: str, table_name: str, dataset_path: str,
                                                dataset_format: str = 'parquet', batch_size: int = 120000, ):
    if ',' in dataset_path: dataset_path = dataset_path.split(',')
    dataset = ds.dataset(dataset_path, format=dataset_format)
    encoder = ArrowToPostgresBinaryEncoder(dataset.schema)
    with conn:
        with conn.cursor() as cursor:
            with cursor.copy(f"COPY {schema_name}.{table_name} FROM STDIN WITH (FORMAT BINARY)") as copy:
                copy.write(encoder.write_header())
                for batch in dataset.to_batches(batch_size=batch_size):
                    copy.write(encoder.write_batch(batch))
                copy.write(encoder.finish())
    conn.close()
