""" File contain all importations of correct for comparison on data pipeline treatement """

import pandas as pd

# data extract from minio instance 
test_data_extraction =  pd.read_parquet('data/correct_data/test_green_tripdata_2024-01.parquet')


# data construct after data modeling
test_data_transform_factable =  pd.read_csv('data/correct_data/fact_table.csv')
test_data_transform_datetime_dim =  pd.read_csv('data/correct_data/datetime_dim.csv')
test_data_transform_passenger_count_dim =  pd.read_csv('data/correct_data/passenger_count_dim.csv')
test_data_transform_payment_type_dim =  pd.read_csv('data/correct_data/payment_type_dim.csv')
test_data_transform_rate_code_dim =  pd.read_csv('data/correct_data/rate_code_dim.csv')
test_data_transform_trip_distance_dim =  pd.read_csv('data/correct_data/trip_distance_dim.csv')


# resume of data modeling on unique entity
test_data_transform_dimensions_dictionnary = {
    "datetime_dim" : test_data_transform_datetime_dim,
    "passenger_count_dim" : test_data_transform_passenger_count_dim,
    "payment_type_dim" : test_data_transform_payment_type_dim,
    "rate_code_dim" : test_data_transform_rate_code_dim,
    "trip_distance_dim" : test_data_transform_trip_distance_dim
}