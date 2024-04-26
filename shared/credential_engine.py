from minio import Minio
from dotenv import load_dotenv
from os import getenv
import psycopg2

# Charger les variables d'environment
load_dotenv()

# psycopg2 engine
engine_db = psycopg2.connect(
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
