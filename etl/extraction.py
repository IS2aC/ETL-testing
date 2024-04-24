from io import BytesIO
import pandas as pd
import pyarrow.parquet as pq

def read_file_from_minio(bucket_name, object_name, minio_client, extension = None):

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