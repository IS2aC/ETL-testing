""" Performance test of datapipeline """

import pytest

from shared.credential_engine import minio_client, engine_db
from etl.etl import ETLFromMinioToPostgresql

class TestEtlPerformance:
    @pytest.fixture()
    def etl_instance(self):
        etl =  ETLFromMinioToPostgresql(
            bucket_name='nyc-cab-data',
            object_name='green_tripdata_2024-01.parquet',
            minio_client=minio_client,
            psotgresql_table_name='fact_table',
            engine_postgresql=engine_db
        )
        return etl

    def test_performance(self, etl_instance, benchmark):
        # Exécuter et mesurer le temps pris par le pipeline ETL
        benchmark(etl_instance.launch_etl)

