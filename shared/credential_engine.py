from minio import Minio
from sqlalchemy import create_engine
from dotenv import load_dotenv
from os import getenv
import psycopg2

# Charger les variables d'environment
load_dotenv()

# engine credentials for : POSTGRESQL
connection_string = f'postgresql+psycopg2://{getenv("POSTGRES_USER")}:{getenv("POSTGRES_PASSWORD")}@{getenv("POSTGRES_HOST")}:{getenv("POSTGRES_PORT")}/{getenv("POSTGRES_DB")}'

# Créez un moteur SQLAlchemy en utilisant la chaîne de connexion
engine_db = create_engine(connection_string)
connection_db = psycopg2.connect(
    host=getenv("POSTGRES_HOST"),
    port = getenv("POSTGRES_PORT"),
    database=getenv("POSTGRES_DB"),
    password=getenv("POSTGRES_PASSWORD"),
    user=getenv("POSTGRES_USER")
)




# engine credentials for  :  MINIO
minio_client =  Minio(
    endpoint="localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

BUCKET_NAME = 'nyc-cab-data'
