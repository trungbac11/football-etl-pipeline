import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
)
        
    try:
        yield client
    except Exception:
        raise
        
class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(datetime.today().strftime("%Y%m%d%H%M%S"),"-".join(context.asset_key.path)
    )
    
        return f"{key}.pq", tmp_file_path    
    
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        try:
            obj.to_parquet(tmp_file_path)
            with connect_minio(self._config) as client:
                client.fput_object(
                    self._config.get("bucket"),
                    key_name,
                    tmp_file_path
                )
            os.remove(tmp_file_path)
        except Exception:
            raise
        
    def load_input(self, context: InputContext) ->  pd.DataFrame:
        # convert to parquet format
        key_name, tmp_file_path = self._get_path(context)
        try:
            with connect_minio(self._config) as client:
                client.fget_object(
                    self._config.get("bucket"),
                    key_name,
                    tmp_file_path
                )
            df = pd.read_parquet(tmp_file_path)
            os.remove(tmp_file_path)
            return df
        except Exception:
            raise    