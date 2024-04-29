import pandas as pd
from etl.transformation import create_fact_table, create_dimension
from etl.model import StorageClient, DatabaseClient

class ETLFromMinioToPostgresql:
    """Principal class of our data pipeline"""

    def __init__(self, minio_client: StorageClient, postgresql_client: DatabaseClient):
        """Constructor"""
        self.minio_client = minio_client
        self.postgresql_client = postgresql_client

    def extract(self) -> pd.DataFrame:
        """Step 1: Extraction"""
        return self.minio_client.read_data()

    def transform(self, df: pd.DataFrame) -> tuple:
        """Step 2: Transformation"""
        fact_table = create_fact_table(df)
        dimensions_on_dictionnary = create_dimension(df)
        return fact_table, dimensions_on_dictionnary

    def load(self, fact_table, dimensions_on_dictionnary):
        """Step 3: Loading"""
        # Loading dimensions
        for key in dimensions_on_dictionnary.keys():
            self.postgresql_client.load_data(df=dimensions_on_dictionnary[key], table_name=key)

        # Loading fact_table
        self.postgresql_client.load_data(df=fact_table, table_name="fact_table")

    def launch_etl(self):
        """Launches the ETL process"""
        # Step 1: Extraction
        df = self.extract()
        print('EXTRACTION OK !')

        # Step 2: Transformation
        fact_table, dimensions_on_dictionnary = self.transform(df)
        print('TRANSFORMATION OK !')

        # Step 3: Loading
        self.load(fact_table, dimensions_on_dictionnary)
        print('CHARGEMENT OK !')

        print("ETL processus terminé avec succès.")
