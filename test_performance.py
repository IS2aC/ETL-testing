""" Performance test of datapipeline """

import pytest
import pytest_benchmark
from shared.credential_engine import minio_client, engine_db
from etl.etl import ETLFromMinioToPostgresql

@pytest.fixture()
def etl_instance():
    etl =  ETLFromMinioToPostgresql(
        bucket_name='nyc-cab-data',
        object_name='green_tripdata_2024-01.parquet',
        minio_client=minio_client,
        psotgresql_table_name='fact_table',
        engine_postgresql=engine_db
    )
    return etl

def test_performance(etl_instance, benchmark):
    # Exécuter et mesurer le temps pris par le pipeline ETL
    result = benchmark(etl_instance.launch_etl)

