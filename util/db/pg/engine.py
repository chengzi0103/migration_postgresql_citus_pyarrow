import sqlalchemy
from sqlalchemy.orm import sessionmaker, scoped_session
import adbc_driver_postgresql.dbapi
import psycopg
def create_postgres_engine(user: str, pwd: str, ip: str, port: str, db: str) -> sqlalchemy.create_engine:
    db_engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{ip}:{port}/{db}".replace(
            "'", ''))
    return db_engine

def create_postgres_session(conn) -> scoped_session:
    session = sessionmaker(bind=conn, autocommit=False, autoflush=False, )
    db_session = scoped_session(session)
    return db_session


def create_postgresql_engine_by_adbc(user: str, pwd: str, ip: str, port: str, db: str):
    url = f"postgresql://{user}:{pwd}@{ip}:{port}/{db}"
    return adbc_driver_postgresql.dbapi.connect(url)


def create_postgresql_cursor_by_adbc(conn):
    return conn.cursor()

def create_postgresql_engine_by_psycopg(user: str, pwd: str, ip: str, port: str, db: str):
    return psycopg.connect(f"postgres://{user}:{pwd}@{ip}:{port}/{db}")

