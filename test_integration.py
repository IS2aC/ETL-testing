import pytest
from etl.etl import ETLFromMinioToPostgresql
from shared.credential_engine import minio_client, engine_db

class TestEtlIntegration:
    """TEST INTEGRATION"""
    @pytest.fixture
    def etl_instance(self):
        # instanciation of etl instance
        etl = ETLFromMinioToPostgresql(
            bucket_name='test-nyc-cab-data',
            object_name='test_green_tripdata_2024-01.parquet',
            minio_client=minio_client, 
            psotgresql_table_name='test_table',
            engine_postgresql=engine_db 
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




