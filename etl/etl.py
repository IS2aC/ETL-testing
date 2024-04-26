from etl.extraction import read_file_from_minio
from etl.transformation import create_fact_table, create_dimension
from etl.loading import load_to_postgresql
import pandas as pd

class ETLFromMinioToPostgresql:
    """principal class of our data pipeline 
    """

    def __init__(self, bucket_name, object_name, minio_client, psotgresql_table_name, engine_postgresql ):
        """constructor
        """
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.minio_client = minio_client
        self.psotgresql_table_name = psotgresql_table_name
        self.engine_postgresql = engine_postgresql


    def extract(self)->pd.DataFrame:
        """ STEP EXTRACTION

        Returns:
            pd.DataFrame: dataframe of data from minio
        """
        df =  read_file_from_minio(bucket_name=self.bucket_name, 
                                   object_name=self.object_name,
                                   minio_client=self.minio_client,
                                   extension='parquet')
        return df
    
    def transform(self, df:pd.DataFrame)->tuple:
        """STEP TRANSFORMATION :  making data modeling

        Args:
            df (pd.DataFrame): dataframe of data extract from minio

        Returns:
            tuple: tuple of fact table and dictionary of dimension for future  data ingestion.
        """
        fact_table = create_fact_table(df)
        dimensions_on_dictionnary = create_dimension(df)

        return fact_table, dimensions_on_dictionnary
    
    def load(self, fact_table, dimensions_on_dictionnary):

        # loading dimensions
        for key in dimensions_on_dictionnary.keys():
            load_to_postgresql(df =  dimensions_on_dictionnary[key], table_name=key, engine=self.engine_postgresql)

        # loading fact_table
        load_to_postgresql(df = fact_table, table_name="fact_table", engine = self.engine_postgresql)


        # loading dimensions_on_dictionnary
        for key in dimensions_on_dictionnary.keys():
            dimensions_on_dictionnary[key].to_csv(f'data/correct_data/{key}.csv', index =  False)


        # load fact_table
        fact_table.to_csv('data/correct_data/fact_table.csv', index =  False)


    def launch_etl(self):
        # Étape 1 : Extraction des données
        df = self.extract() 
        print('EXTRACTION OK !')

        # Étape 2 : Transformation des données
        fact_table, dimensions_on_dictionnary = self.transform(df)
        print('TRANSFORMATION OK !')
        
        # Étape 3 : Chargement des données transformées dans PostgreSQL
        self.load(fact_table, dimensions_on_dictionnary)
        print('CHARGEMENT OK !')

        print("ETL processus terminé avec succès.")


    


