import pandas as pd 
from datetime import datetime

def load_to_postgresql(df : pd.DataFrame, engine, table_name) -> None:
    pass
    

def load_to_csv(df:pd.DataFrame):

    df.to_csv(f'loading_space/data_{datetime.now()}.csv', index =  False)

    

