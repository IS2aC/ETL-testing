import pytest
from etl.etl import ETLFromMinioToPostgresql
from shared.credential_engine import minio_client, engine_db
from etl.model import PostgresqlClient, MinioClient

mc =  MinioClient(bucket_name = 'nyc-cab-data', object_name = 'green_tripdata_2024-01.parquet', minio_client = minio_client)
pc = PostgresqlClient(engine = engine_db)

class TestEtlIntegration:
    """TEST INTEGRATION"""
    @pytest.fixture
    def etl_instance(self):
        # instanciation of etl instance
        etl = ETLFromMinioToPostgresql(
            minio_client = mc,
            postgresql_client = pc
        )
        return etl

    def test_launch_etl(self, etl_instance):
        # launch etl
        etl_instance.launch_etl()
        
        # extract verifications
        extracted_data = etl_instance.extract()
        assert not extracted_data.empty # global check on dataframe extract

        # transform verifications
        fact_table, dimensions_dictionnary = etl_instance.transform(extracted_data)
        assert not fact_table.empty # global check on dataframe fact table
        assert isinstance(dimensions_dictionnary, dict) # global type check over dimensions dictionary entity


        # load verifications




