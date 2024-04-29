import pytest
from etl.etl import ETLFromMinioToPostgresql
from shared.credential_engine import minio_client, engine_db
from correct_data import test_data_extraction, test_data_transform_factable, test_data_transform_dimensions_dictionnary
import pandas as pd 
from etl.model import MinioClient, PostgresqlClient

mc =  MinioClient(bucket_name = 'nyc-cab-data', object_name = 'green_tripdata_2024-01.parquet', minio_client = minio_client)
pc = PostgresqlClient(engine = engine_db)


class TestEtlExtraction:
    @pytest.fixture
    def etl_instance(self):
        return ETLFromMinioToPostgresql(
            minio_client=mc,
            postgresql_client=pc
        )

    @pytest.fixture
    def extracted_data(self, etl_instance):
        return etl_instance.extract()

    def test_extraction_shape(self, extracted_data):
        assert extracted_data.shape == test_data_extraction.shape

    def test_extraction_columns(self, extracted_data):
        assert set(extracted_data.columns) == set(test_data_extraction.columns)

    def test_extraction_unique_values(self, extracted_data):
        for col in extracted_data.columns:
            assert len(extracted_data[col].unique()) == len(test_data_extraction[col].unique())

class TestEtlTransformation:
    @pytest.fixture
    def etl_instance(self):
        return ETLFromMinioToPostgresql(
            minio_client=mc,
            postgresql_client=pc
        )

    @pytest.fixture
    def extracted_data(self, etl_instance):
        return etl_instance.extract()

    @pytest.fixture
    def transformed_data(self, etl_instance, extracted_data):
        return etl_instance.transform(extracted_data)

    def test_transformation_return_types(self, transformed_data):
        fact_table, dimensions_dictionnary = transformed_data
        assert isinstance(fact_table, pd.DataFrame)
        assert isinstance(dimensions_dictionnary, dict)

    def test_transformation_dimensions(self, transformed_data):
        fact_table, dimensions_dictionnary = transformed_data
        assert fact_table.shape == test_data_transform_factable.shape

        for key in dimensions_dictionnary.keys():
            assert dimensions_dictionnary[key].shape == test_data_transform_dimensions_dictionnary[key].shape

    def test_transformation_unique_values(self, transformed_data):
        fact_table, dimensions_dictionnary = transformed_data

        for col in fact_table.columns:
            assert len(fact_table[col].unique()) == len(test_data_transform_factable[col].unique())

        for key in dimensions_dictionnary.keys():
            for col in dimensions_dictionnary[key].columns:
                assert len(dimensions_dictionnary[key][col].unique()) == len(test_data_transform_dimensions_dictionnary[key][col].unique())

class TestEtlLoad:
    @pytest.fixture
    def etl_instance(self):
        return ETLFromMinioToPostgresql(
            minio_client=mc,
            postgresql_client=pc
        )

    @pytest.fixture
    def extracted_data(self, etl_instance):
        return etl_instance.extract()

    @pytest.fixture
    def transformed_data(self, etl_instance, extracted_data):
        return etl_instance.transform(extracted_data)

    def test_load_process_runs_without_errors(self, etl_instance, transformed_data):
        fact_table, dimensions_dictionnary = transformed_data
        # Running the load function, which should not raise an exception
        try:
            etl_instance.load(fact_table, dimensions_dictionnary)
            assert True
        except Exception as e:
            pytest.fail(f"Load process raised an exception: {e}")