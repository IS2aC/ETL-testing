from shared.credential_engine import engine_db
import pandas as pd
from etl.loading import load_to_postgresql

# extraction des données se fait : psycopg2
# df =  pd.read_sql("select * from test_table;", connection_db)

# load avec psycopg2
df =  pd.DataFrame({
    'col1':[f'value{i}' for i in range(3)],
    'col2':[i for i in range(3)]
})




load_to_postgresql(df =  df, connection_db=engine_db, table_name='test_table_test_test')
