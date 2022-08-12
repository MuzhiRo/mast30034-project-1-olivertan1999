from pyspark.sql import functions as func
import pandas as pd
import numpy as np


def rename_col(df):
    """Rename columns in data to make all variable names consistent"""
    
    column_name = {'VendorID': 'vendor_id', 
                   'RatecodeID': 'rate_code_id', 
                   'PULocationID': 'pu_location_id',
                   'DOLocationID': 'do_location_id'}

    for key, value in column_name.items():
        df = df.withColumnRenamed(key, value)
        
    return df

def extract_features(df):
    """Extract trip duration in minutes, pickup date & hour, dropoff date & hour as standalone feature"""
    
    # Extract trip duration in minutes
    df = df.withColumn('trip_duration_min',
                       (func.col('tpep_dropoff_datetime').cast('long') - 
                        func.col('tpep_pickup_datetime').cast('long')) / 60)

    # Extract date and hour as standalone feature
    df = (df
      .withColumn("pickup_date", func.col("tpep_pickup_datetime").cast("date"))
      .withColumn("pickup_hour", func.hour(func.col("tpep_pickup_datetime")))
      .withColumn("dropoff_date", func.col("tpep_dropoff_datetime").cast("date"))
      .withColumn("dropoff_hour", func.hour(func.col("tpep_dropoff_datetime")))
    )
    
    return df


def filter_data(df):
    """Filter data based on the outlier analysis done on preprocessing.ipynb"""
    
    # Filter based on the minimum possible values for numerical features
    df1 = df.where((func.col('passenger_count') > 0) &
                   (func.col('trip_distance') > 0.5) &
                   (func.col('trip_duration_min') > 1) &
                   (func.col('fare_amount') >= 2.50) &
                   (func.col('extra') >= 0) &
                   (func.col('mta_tax') >= 0) &
                   (func.col('tip_amount') >= 0) &
                   (func.col('tolls_amount') >= 0) &
                   (func.col('improvement_surcharge') >= 0) &
                   (func.col('total_amount') >= 0) &
                   (func.col('congestion_surcharge') >= 0) &
                   (func.col('airport_fee') >= 0))


    # Filter the columns to cover 99.99 percentile of the data for numerical features
    df1 = df1.where((func.col('fare_amount') <= df1.selectExpr('percentile(fare_amount, 0.9999)').collect()[0][0]) &
                    (func.col('trip_distance') <= df1.selectExpr('percentile(trip_distance, 0.9999)').collect()[0][0]) &
                    (func.col('tip_amount') <= df1.selectExpr('percentile(tip_amount, 0.9999)').collect()[0][0]) &
                    (func.col('total_amount') <= df1.selectExpr('percentile(total_amount, 0.9999)').collect()[0][0]) &
                    (func.col('tolls_amount') <= df1.selectExpr('percentile(tolls_amount, 0.9999)').collect()[0][0]) &
                    (func.col('trip_duration_min') <= 300))

    # Filter the period to between 2021-10 to 2022-04
    df1 = df1.where((func.col("pickup_date") >= '2021-10-01') & 
                    (func.col("dropoff_date") >= '2021-10-01') &
                    (func.col("pickup_date") <= '2022-04-30') & 
                    (func.col("dropoff_date") <= '2022-04-30'))
    
    return df1


    