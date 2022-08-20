from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import shapefile as shp
import pandas as pd
import numpy as np
from .utility import rename_col, extract_features, filter_data


def preprocess(sdf, cab_type='yellow'):
    """Preprocess raw TLC taxi dataset and transform it to a suitable form for prediction."""
    
    # Rename columns
    renamed_sdf = rename_col(sdf, cab_type)

    # Extract features
    features_extracted_sdf = extract_features(renamed_sdf)

    # Filter outliers
    filtered_sdf = filter_data(features_extracted_sdf, cab_type)
    
    return filtered_sdf

def transform_demand(sdf, time_format='date'):
    """Group data by location and time format and count the number of records."""
    
    if time_format == 'date':
        
        # Aggregate and count number of daily instances in each location id
        pickup_demand = sdf.groupBy("pu_location_id", "pickup_date").count()
        dropoff_demand = sdf.groupBy("do_location_id", "dropoff_date").count()
        
    elif time_format == 'hour':
        
        # Aggregate and count number of hourly instances in each location id
        pickup_demand = sdf.groupBy("pu_location_id", "pickup_date", "pickup_hour").count()
        dropoff_demand = sdf.groupBy("do_location_id", "dropoff_date", "dropoff_hour").count()
    
    
    return pickup_demand, dropoff_demand
    
