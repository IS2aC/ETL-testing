from etl.extraction import read_file_from_minio
from etl.transformation import create_fact_table, create_dimension
from etl.loading import load_to_postgresql, load_to_csv


class ETLFromMinioToPostgresql:

    def __init__(self, bucket_name, object_name, minio_client, psotgresql_table_name, engine_postgresql ):
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.minio_client = minio_client
        self.psotgresql_table_name = psotgresql_table_name
        self.engine_postgresql = engine_postgresql


    def extract(self):
        df =  read_file_from_minio(bucket_name=self.bucket_name, 
                                   object_name=self.object_name,
                                   minio_client=self.minio_client,
                                   extension='parquet')
        return df
    
    def transform(self, df):
        fact_table = create_fact_table(df)
        # dimensions_on_dictionnary = create_dimension(df)
        return fact_table
    
    def load(self, fact_table):
        # loading fact_table
        # load_to_postgresql(df = fact_table, table_name="fact_table", engine = self.engine_postgresql)
        load_to_csv(df = fact_table)


    def launch_etl(self):
        # Étape 1 : Extraction des données
        df = self.extract() 
        print('EXTRACTION OK !')

        # Étape 2 : Transformation des données
        fact_table = self.transform(df)
        print('TRANSFORMATION OK !')
        
        # Étape 3 : Chargement des données transformées dans PostgreSQL
        self.load(fact_table)
        print('CHARGEMENT OK !')

        print("ETL processus terminé avec succès.")


    


