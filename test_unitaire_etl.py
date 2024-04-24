""" TEST UNITAIRE """
import pytest
import pandas as pd
from shared.credential_engine import minio_client
from shared.credential_engine import engine_db
from etl.etl import ETLFromMinioToPostgresql

test_data_extraction =  pd.read_parquet('DataForTest/test_green_tripdata_2024-01.parquet')
test_data_transform =  pd.read_csv('loading_space/data_2024-04-24 18:51:54.615088.csv')

class TestEtlExtraction:
    @pytest.fixture
    def etl_extraction(self):
        etl_class =  ETLFromMinioToPostgresql(
            bucket_name= 'nyc-cab-data',
            object_name='green_tripdata_2024-01.parquet',
            minio_client= minio_client,
            psotgresql_table_name='fact_table',
            engine_postgresql= engine_db
        )

        data = etl_class.extract()
        return data


    def test_extraction_check_dimension(self, etl_extraction):
        
        assert etl_extraction.shape == test_data_extraction.shape

    def test_extraction_check_columns(self, etl_extraction):

        assert  set(etl_extraction.columns) == set(test_data_extraction.columns)

    def test_extraction_unique_values_from_columns(self, etl_extraction):

        for col in etl_extraction.columns:
            assert len(etl_extraction[col].unique()) == len(test_data_extraction[col].unique())

class TestEtlTransform:
    @pytest.fixture
    def etl_transform(self):
        etl_class =  ETLFromMinioToPostgresql(
            bucket_name= 'nyc-cab-data',
            object_name='green_tripdata_2024-01.parquet',
            minio_client= minio_client,
            psotgresql_table_name='fact_table',
            engine_postgresql= engine_db
        )

        data_extract =  etl_class.extract()

        data_transform = etl_class.transform(data_extract)
        return data_transform


    def test_transform_check_dimension(self, etl_transform):

        assert etl_transform.shape == test_data_transform.shape

    def test_transform_check_columns(self, etl_transform):

        assert set(etl_transform.shape) == set(test_data_transform.shape)

    def test_transform_unique_values_from_columns(self, etl_transform):
        for col in etl_transform.columns:
            assert len(etl_transform[col].unique()) == len(test_data_transform[col].unique())




    



