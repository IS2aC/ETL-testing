from etl.etl import ETLFromMinioToPostgresql
from shared.credential_engine import engine_db, minio_client


if __name__ == '__main__':

    extract_transform_load =  ETLFromMinioToPostgresql(
        bucket_name= 'nyc-cab-data',
        object_name='green_tripdata_2024-01.parquet',
        minio_client= minio_client,
        psotgresql_table_name='fact_table',
        engine_postgresql= engine_db
    ).launch_etl()