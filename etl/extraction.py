from io import BytesIO
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

def read_file_from_minio(bucket_name:str, object_name:str, minio_client:Minio, extension:str = None) -> pd.DataFrame:
    """make extraction of a dataframe from minio bucket

    Args:
        bucket_name (str): bucket where object name stay.
        object_name (str): name of object to extract
        minio_client (Minio): credentials to access on api of minio
        extension (str, optional): type of file to extract(ex: csv, avro, parquet, etc..). Defaults to None.

    Returns:
        pd.DataFrame: dataframe of data extract on object storage space.
    """

    try:
        # Télécharger l'objet depuis MinIO
        response = minio_client.get_object(bucket_name, object_name)
        
        # Lire les données de l'objet
        file_data = response.read()
        
        # Créer un flux de mémoire à partir des données
        file_stream = BytesIO(file_data)
        
        # Vérifier l'extension du fichier
        if extension == 'csv':
            # Lire un fichier CSV
            df = pd.read_csv(file_stream)
            return df
        elif extension =='xlsx':
            # Lire un fichier Excel
            df = pd.read_excel(file_stream)
            return df
        elif extension == 'parquet':
            # Lire un fichier Parquet
            parquet_table = pq.read_table(file_stream)
            df = parquet_table.to_pandas()
            return df
        else:
            print("Format de fichier non pris en charge.")
            return None
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier depuis MinIO : {e}")
        return None