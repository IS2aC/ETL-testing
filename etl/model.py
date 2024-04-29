""" Factory space !"""
from etl.extraction import read_file_from_minio
from etl.loading import load_to_postgresql
from abc import ABC, abstractmethod

class StorageClient(ABC):
    @abstractmethod
    def read_data(self):
        pass

class DatabaseClient(ABC):
    @abstractmethod
    def load_data(self, df):
        pass

class MinioClient(StorageClient):
    def __init__(self, bucket_name, object_name, minio_client):
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.minio_client = minio_client

    def read_data(self, extension='parquet'):
        # Simulation de la lecture de fichier depuis Minio
        return read_file_from_minio(bucket_name=self.bucket_name,
                                    object_name=self.object_name,
                                    minio_client=self.minio_client,
                                    extension=extension)

class PostgresqlClient(DatabaseClient):
    def __init__(self, engine):
        self.engine = engine

    def load_data(self, df, table_name):
        # Simulation du chargement de données dans PostgreSQL
        load_to_postgresql(df=df, connection_db=self.engine, table_name=table_name)