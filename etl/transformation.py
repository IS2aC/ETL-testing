import pandas as pd 

# fonctions de creation des dimensions
def create_datetime_dim(df:pd.DataFrame) -> pd.DataFrame:
    """ make date dimension from data extract on minio

    Args:
        df (pd.DataFrame): dataframe of data from minio

    Returns:
        pd.DataFrame: date dimenion for data modeling
    """
    try :
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        datetime_dim = df[['lpep_pickup_datetime','lpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
        datetime_dim['pick_hour'] = datetime_dim['lpep_pickup_datetime'].dt.hour
        datetime_dim['pick_day'] = datetime_dim['lpep_pickup_datetime'].dt.day
        datetime_dim['pick_month'] = datetime_dim['lpep_pickup_datetime'].dt.month
        datetime_dim['pick_year'] = datetime_dim['lpep_pickup_datetime'].dt.year
        datetime_dim['pick_weekday'] = datetime_dim['lpep_pickup_datetime'].dt.weekday
        
        datetime_dim['drop_hour'] = datetime_dim['lpep_dropoff_datetime'].dt.hour
        datetime_dim['drop_day'] = datetime_dim['lpep_dropoff_datetime'].dt.day
        datetime_dim['drop_month'] = datetime_dim['lpep_dropoff_datetime'].dt.month
        datetime_dim['drop_year'] = datetime_dim['lpep_dropoff_datetime'].dt.year
        datetime_dim['drop_weekday'] = datetime_dim['lpep_dropoff_datetime'].dt.weekday

        datetime_dim['datetime_id'] = datetime_dim.index

        datetime_dim = datetime_dim[['datetime_id', 'lpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'lpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
        return datetime_dim
    except:
        print("Données non-conforme ! ")

def create_passenger_count_dim(df:pd.DataFrame) -> pd.DataFrame:
    """make passenger count dimension from data extract on minio

    Args:
        df (pd.DataFrame): datframe of data from minio

    Returns:
        pd.DataFrame: passenger count dimension for data modeling
    """

    try : 
        passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
        passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
        passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    
        # construct  passenger_count_dim
        return passenger_count_dim
    except:
        print("Données non-conforme ! ")

def create_trip_distance_dim(df:pd.DataFrame) -> pd.DataFrame:
    """make trip distance dimension from dataframe extract on minio

    Args:
        df (pd.DataFrame): datframe of data from minio

    Returns:
        pd.DataFrame: trip distance dimension for data modeling
    """
    try : 
        trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
        trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
        trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
        return  trip_distance_dim
    except:
        print('Données non-conforme ! ')

def create_rate_code_dim(df:pd.DataFrame) -> pd.DataFrame:
    """make rate code dimension from data extract on minio

    Args:
        df (pd.DataFrame): datframe of data from minio

    Returns:
        pd.DataFrame: rate code dimension for data modeling
    """
    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }
    try : 
        rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
        rate_code_dim['rate_code_id'] = rate_code_dim.index
        rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
        rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]
        return rate_code_dim
    except:
        print('Données non-conforme ! ')

def create_payment_type_dim(df:pd.DataFrame) -> pd.DataFrame:
    """make payment type dimension from data extract on minio
    Args:
        df (pd.DataFrame): dataframe of data from minio

    Returns:
        pd.DataFrame: payemnt type dimension for data modeling
    """
    
    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }
    try : 
        payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
        payment_type_dim['payment_type_id'] = payment_type_dim.index
        payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
        payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
        return payment_type_dim
    except:
        print('Données non-conforme ! ')

####################################################################################################################################################################################
# all dimenssions in one element !
def create_dimension(df:pd.DataFrame)-> dict:
    """make a dictionary of all dimensions on our data modeling
    as dimension's name  on key and dimension's dataframe on value

    Args:
        df (pd.DataFrame): dataframe of data extract on minio

    Returns:
        pd.DataFrame: dictionary of dimensionof all dimensions for data modeling on one entity.
    """

    try:
        dimensions_on_dictionnary =  {
            "datetime_dim" : create_datetime_dim(df),
            "passenger_count_dim" : create_passenger_count_dim(df),
            "trip_distance_dim" : create_trip_distance_dim(df),
            "rate_code_dim" : create_rate_code_dim(df),
            "payment_type_dim" : create_payment_type_dim(df)
        }

        return dimensions_on_dictionnary
    except :
        print('Données non-conforme ! ')

# fonctions de creation de la table de faits
def create_fact_table(df:pd.DataFrame)->pd.DataFrame:
    """make fact table from data extract on minio

    Args:
        df (pd.DataFrame): dataframe of data extract on minio

    Returns:
        pd.DataFrame: fact table of our data modeling
    """

    try:
        datetime_dim = create_datetime_dim(df)
        passenger_count_dim = create_passenger_count_dim(df)
        trip_distance_dim = create_trip_distance_dim(df)
        rate_code_dim = create_rate_code_dim(df)
        payment_type_dim = create_payment_type_dim(df)
        fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                     .merge(trip_distance_dim, on='trip_distance') \
                     .merge(rate_code_dim, on='RatecodeID') \
                     .merge(datetime_dim, on=['lpep_pickup_datetime','lpep_dropoff_datetime']) \
                     .merge(payment_type_dim, on='payment_type') \
                     [['VendorID', 'datetime_id', 'passenger_count_id',
                       'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag',
                       'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                       'improvement_surcharge', 'total_amount']]
        return fact_table
    except :
        print('Données non-conforme ! ')