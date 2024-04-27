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


class TestEtlLoad:
    @pytest.fixture
    def etl_load(self):
        # Setup for the ETL Load test
        etl_class = ETLFromMinioToPostgresql(
            bucket_name='nyc-cab-data',
            object_name='green_tripdata_2024-01.parquet',
            minio_client=minio_client,  # This should be mocked or a real initialized minio client
            psotgresql_table_name='fact_table',
            engine_postgresql=engine_db  # This should be a connection or a pool connected to a test database
        )
        df = etl_class.extract()
        fact_table, dimensions_dictionnary = etl_class.transform(df)
        return etl_class, fact_table, dimensions_dictionnary

    def test_load_process_runs_without_errors(self, etl_load):
        etl_class, fact_table, dimensions_dictionnary = etl_load
        # Running the load function, which should not raise an exception
        try:
            etl_class.load(fact_table, dimensions_dictionnary)
            assert True
        except Exception as e:
            pytest.fail(f"Load process raised an exception: {e}")

    # drop data on database first
    # def test_load_check_dimensions(self):
    #     assert test_data_transform_factable.shape == pd.read_sql('select * from fact_table', engine_db).shape
        # possible to make the same test for all table on our database


    




