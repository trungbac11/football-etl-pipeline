from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext 
from sqlalchemy import create_engine

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    
    engine = create_engine(conn_info)
    connection = engine.raw_connection()
    
    try:
        yield connection
    finally:
        connection.close()
    
    
class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        pass
    
    def load_input(self, context: InputContext) -> None:
        pass
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data