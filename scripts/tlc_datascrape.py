import os
from urllib.request import urlretrieve

def get_tlc_data():
    '''Retrieve yellow and green taxi cab records from 2021-10 to 2022-04 and
    output them to the data/raw folders'''

    output_relative_dir = '../data/raw/'

    # Check if it exists as it makedir will raise an error if it does exist
    if not os.path.exists(output_relative_dir):
        os.makedirs(output_relative_dir)


    if not os.path.exists(output_relative_dir + 'tlc_data'):
        os.makedirs(output_relative_dir + 'tlc_data')
    
    # Specify which year and month to collect data from
    date = {'2021':[10,11,12],
            '2022':[1,2,3,4]}
    
    # This is the URL template as of 08/2022
    trip_url = {'yellow':"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_",
                'green':"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_"}


    # Make individual directory for each cab type
    for cab_type in trip_url.keys():
        if not os.path.exists(output_relative_dir + 'tlc_data/' + cab_type):
            os.makedirs(output_relative_dir + 'tlc_data/' + cab_type)
            
    # Data output directory is `data/tlc_data/`
    tlc_output_dir = output_relative_dir + 'tlc_data/'

    for year in date.keys():
        for month in date[year]:
            # 0-fill i.e 1 -> 01, 2 -> 02, etc
            month = str(month).zfill(2) 
            print(f"Begin month {month}")

            for cab_type, url in trip_url.items():
                # Generate url 
                get_url = f'{url}{year}-{month}.parquet'
                # Generate output location and filename
                output_dir = f"{tlc_output_dir}{cab_type}/{year}-{month}.parquet"
                # Download
                urlretrieve(get_url, output_dir)


            print(f"Completed month {month}")
            
            