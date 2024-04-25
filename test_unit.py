""" TEST UNITAIRE """
import pytest
from shared.credential_engine import minio_client
from shared.credential_engine import engine_db
from etl.etl import ETLFromMinioToPostgresql
from correct_data import test_data_extraction, test_data_transform_factable, test_data_transform_dimensions_dictionnary
import pandas as pd 



class TestEtlExtraction:
    @pytest.fixture
    def etl_extraction(self):
        etl_class =  ETLFromMinioToPostgresql(
            bucket_name = 'nyc-cab-data',
            object_name ='green_tripdata_2024-01.parquet',
            minio_client = minio_client,
            psotgresql_table_name ='fact_table',
            engine_postgresql = engine_db
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

    def test_extraction_data_alteration(self, etl_extraction):
        pass


class TestEtlTransformation:
    @pytest.fixture
    def etl_transform(self):
        etl_class = ETLFromMinioToPostgresql(
            bucket_name='nyc-cab-data',
            object_name='green_tripdata_2024-01.parquet',
            minio_client=minio_client,
            psotgresql_table_name='fact_table',
            engine_postgresql=engine_db
        )

        data = etl_class.extract()
        data_transform, dimensions_dictionnary = etl_class.transform(data)
        return data_transform, dimensions_dictionnary

    def test_transform_check_return_transformation_step(self, etl_transform):
        fact_table, dimensions_dictionnary = etl_transform

        # checking return transformation step
        assert isinstance(fact_table, pd.DataFrame)
        assert isinstance(dimensions_dictionnary, dict)

    def test_transform_check_dimensions(self, etl_transform):
        fact_table, dimensions_dictionnary = etl_transform

        # checking dimension of table fact_table
        assert fact_table.shape == test_data_transform_factable.shape

        # checking dimension of dimensions table of our data modeling
        for key in dimensions_dictionnary.keys():
            assert dimensions_dictionnary[key].shape == test_data_transform_dimensions_dictionnary[key].shape


    def test_traansform_unique_values_from_columns(self, etl_transform):
        fact_table, dimensions_dictionnary = etl_transform

        # checking unique values on all columns of our fact_table
        for col in fact_table.columns:
            assert len(fact_table[col].unique()) == len(test_data_transform_factable[col].unique())

        # checking unique values on all columns of our dimensions table from data modeling
        for key in dimensions_dictionnary.keys():
            for col in dimensions_dictionnary[key].columns:
                assert len(dimensions_dictionnary[key][col].unique()) == len(test_data_transform_dimensions_dictionnary[key][col].unique())




    




