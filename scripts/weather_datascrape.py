#!/usr/bin/env python
# coding: utf-8

# # NOAA Weather Data Scrape
# 
# In this section, we will retrieve and query the weather data in the NYC through NOAA (National Oceanic and Atmospheric Administration). 
# 
# 2021 Hourly Data Collected at Central Park NY : [NY CENTRAL PARK 2021 WEATHER DATA](https://www.ncei.noaa.gov/data/global-hourly/access/2021/72505394728.csv)
# 
# 2022 Hourly Data Collected at Central Park NY : [NY CENTRAL PARK 2021 WEATHER DATA](https://www.ncei.noaa.gov/data/global-hourly/access/2022/72505394728.csv)
# 
# 
# ### Aim: 
# - Join this data to the hourly pickup data to predict future hourly taxi ride demand.
# 
# ### Data dictionary:
# - Can be retrieved from this link: [FEDERAL CLIMATE COMPLEX DATA DOCUMENTATION FOR INTEGRATED SURFACE DATA](https://www.ncei.noaa.gov/data/global-hourly/doc/isd-format-document.pdf) 

# In[1]:


import requests
import pandas as pd
import json
import numpy as np
import datetime as dt

# TOKEN = 'prFURygHhcjchMdwFdWXiQwJyTzpWoDf'
# STATION_ID = 'GHCND:USW00094728'


# In[2]:


df2021 = pd.read_csv("https://www.ncei.noaa.gov/data/global-hourly/access/2021/72505394728.csv")
df2022 = pd.read_csv("https://www.ncei.noaa.gov/data/global-hourly/access/2022/72505394728.csv")


# In[17]:


df = pd.concat([df2021, df2022])


# In[18]:


print(f"Number of instances: {len(df)}")
print(f"Number of features : {len(df.columns)}")


# In[21]:


# Narrow down to the required period
df = df[(df['DATE'] >= '2021-10-01') & (df['DATE'] < '2022-05-01')]


# In[23]:


print(f"Number of instances within required period: {len(df)}")


# In[8]:


def preprocess(hourly_data):
    '''Extract, clean and unscale the data according to the data 
    dictionary provided.'''
    
    # Get the hourly weather report type
    df = hourly_data.loc[hourly_data['REPORT_TYPE'] == 'FM-15', :]
    
    # Extract the unscaled values for each column
    df.loc[:,'WND'] = (df['WND'].apply(lambda x: int(x.split(',')[-2])/10)
                                .replace(999.9, np.nan))
    
    df.loc[:,'TMP'] = (df['TMP'].apply(lambda x: int(x.split(',')[0])/10)
                                .replace(999.9, np.nan))
    
    df.loc[:,'DEW'] = (df['DEW'].apply(lambda x: int(x.split(',')[0])/10)
                                .replace(999.9, np.nan))
    
    df.loc[:,'SLP'] = (df['SLP'].apply(lambda x: int(x.split(',')[0])/10)
                                .replace(9999.9, np.nan))
    
    # Impute missing data using data from an hour before
    df.ffill(inplace=True)
    
    # Filter data to period between 2021-10 to 2022-04
    processed_data = df.loc[(df['DATE'] <= '2022-05-01') & (df['DATE'] >= '2021-10-01'), :]
    
    # Extract date and hour from datetime column
    processed_data.loc[:,'date'] = pd.to_datetime(processed_data['DATE'])
    processed_data.loc[:,'hour'] = processed_data['date'].dt.hour
    processed_data.loc[:,'date'] = processed_data['date'].dt.date
    
    processed_data.rename({'WND':'wind_speed',
                           'TMP':'temperature',
                           'DEW':'dew_point',
                           'SLP':'atmospheric_pressure'})
    
    
    return processed_data[['date',
                           'hour',
                           'TMP',
                           'DEW',
                           'SLP',
                           'AA1']]
    


# In[9]:


print(f"Number of instances we require: {(31+30+31+31+28+31+30)*24}")


# In[10]:


df2 = preprocess(df)


# In[12]:


print(f"Number of instances after preprocessing: {len(df2)}")


# In[254]:


# Store the data in the curated folder
df2.to_csv("../data/curated/hourly_weather.csv")


# In[253]:


df2


# In[ ]:




