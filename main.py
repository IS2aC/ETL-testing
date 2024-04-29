from etl.etl import ETLFromMinioToPostgresql
from shared.credential_engine import engine_db, minio_client
from etl.model import MinioClient, PostgresqlClient


engine_minio =  MinioClient(bucket_name='nyc-cab-data', object_name='green_tripdata_2024-01.parquet', minio_client=minio_client)
engine_postgres = PostgresqlClient(engine=engine_db)

if __name__ == '__main__':

    extract_transform_load =  ETLFromMinioToPostgresql(
        minio_client=engine_minio,
        postgresql_client=engine_postgres
    ).launch_etl()