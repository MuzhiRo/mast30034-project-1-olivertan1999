from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import shapefile as shp
import pandas as pd
import numpy as np
from .utility import rename_col, extract_features, filter_data


def preprocess(sdf):
    """Preprocess raw TLC taxi dataset and transform it to a suitable form for prediction."""
    
    # Rename columns
    renamed_sdf = rename_col(sdf)

    # Extract features
    features_extracted_sdf = extract_features(renamed_sdf)

    # Filter outliers
    filtered_sdf = filter_data(features_extracted_sdf)
    
    return sdf

def transform_daily_demand(sdf):
    """Group data by location and date and count the number of records."""
    
    # Aggregate and count number of daily instances in each location id
    pickup_daily_demand = sdf.groupBy("pu_location_id", "pickup_date").count()
    dropoff_daily_demand = sdf.groupBy("do_location_id", "dropoff_date").count()
    
    return pickup_daily_demand, dropoff_daily_demand
    
    
def transform_hourly_demand(sdf):
    """Group data by location, date and hour and count the number of records."""
    
    # Aggregate and count number of hourly instances in each location id
    pickup_hourly_demand = sdf.groupBy("pu_location_id", "pickup_date", "pickup_hour").count()
    dropoff_hourly_demand = sdf.groupBy("do_location_id", "dropoff_date", "dropoff_hour").count()
    
    return pickup_hourly_demand, dropoff_hourly_demand
