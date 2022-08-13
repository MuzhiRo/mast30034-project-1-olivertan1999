from sodapy import Socrata
import pandas as pd
import geopandas as gpd


def retrieve_pluto_data():
    """Retrieve and preprocess the PLUTO dataset."""

    # Access NYC Open Data Api
    app_token = 'UMXzWyQUIWIjSyhP5AKzUew4x'

    client = Socrata("data.cityofnewyork.us", app_token)
    
    # Retrieve the latest PLUTO data 
    query = """
    SELECT
        borough, 
        landuse, 
        BldgClass,
        latitude, 
        longitude,
        zipcode,
        version,
        UnitsTotal,
        UnitsRes
    WHERE landuse IS NOT NULL 
      AND version = '22v1'
    LIMIT 1000000
    """

    results = client.get("64uk-42ks", query=query)
    
    # Read result and convert to geopandas dataframe format
    results_df = pd.DataFrame.from_records(results)

    results_gdf = gpd.GeoDataFrame(
        results_df,
        geometry=gpd.points_from_xy(results_df.longitude, results_df.latitude)
    )
    
    # Extract only the general use of the lot 
    results_gdf['BldgClass'] = results_gdf['BldgClass'].apply(lambda x: x[0])

    # Convert the UnitsTotal and UnitsRes to numeric
    results_gdf[['UnitsTotal', 'UnitsRes']] = results_gdf[['UnitsTotal', 'UnitsRes']].apply(pd.to_numeric)
    
    results_gdf['non_residential_units'] = results_gdf['UnitsTotal'] - results_gdf['UnitsRes']
    
    # Rename columns for consistency
    results_gdf.rename({'UnitsRes':'residential_units',
                        'BldgClass':'building_class',
                        'UnitsTotal':'total_units'},
                       axis = 1,
                       inplace = True)

    # Reorder columns and drop irrelevant columns
    results_gdf = results_gdf[['borough',
                               'landuse',
                               'building_class',
                               'residential_units',
                               'non_residential_units',
                               'total_units',
                               'zipcode',
                               'geometry']]


    return results_gdf


def get_merged_zone_data(sf, zone):
    """
    Get spatially joined dataset between PLUTO and TLC Zone dataset.
    
    Param:
        sf: TLC zone shapefile in geopandas format
        zone: TLC zone lookup in pandas format
        
    Output:
        dataframe: spatially joined dataset between PLUTO and TLC Zone datasets.
        
    """
    
    pluto_gdf = retrieve_pluto_data()

    # Convert the geometry shape to longitude and latitude
    sf['geometry'] = sf['geometry'].to_crs("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
    
    # Merge the zone lookup csv and the shapefile
    tlc_gdf = gpd.GeoDataFrame(
        pd.merge(zone, sf, on='LocationID', how='inner')
    ).drop(['Zone','Borough'], axis=1)
    
    # Spatial join on the pluto and tlc zone data using the 'within' operator
    sjoined_df = gpd.sjoin(
        pluto_gdf, tlc_gdf, op='within'
    ).drop(['index_right','borough_left'], axis=1)

    # Rename columns for consistency
    sjoined_df = sjoined_df.rename({'OBJECTID':'object_id',
                                    'LocationID':'location_id',
                                    'borough_right':'borough'})
    
    return sjoined_df

    
    
    
    
    
    