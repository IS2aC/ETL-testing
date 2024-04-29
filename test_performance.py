""" Performance test of datapipeline """

import pytest

from shared.credential_engine import minio_client, engine_db
from etl.etl import ETLFromMinioToPostgresql
from etl.model import MinioClient, PostgresqlClient

mc =  MinioClient(bucket_name = 'nyc-cab-data', object_name = 'green_tripdata_2024-01.parquet', minio_client = minio_client)
pc = PostgresqlClient(engine = engine_db)

class TestEtlPerformance:
    @pytest.fixture()
    def etl_instance(self):
        etl =  ETLFromMinioToPostgresql(
            minio_client= mc, postgresql_client=pc
        )
        return etl

    def test_performance(self, etl_instance, benchmark):
        # Exécuter et mesurer le temps pris par le pipeline ETL
        benchmark(etl_instance.launch_etl)

