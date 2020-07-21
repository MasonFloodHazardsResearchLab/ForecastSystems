# The purpose of this code is to download and save
# stage and streamflow forecast data from AHPS stations
# Author: Gustavo Coelho
# Jul, 2020


######################################################################
## Library
import os
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
from datetime import datetime, timezone, timedelta


######################################################################
## Settings

# AHPS stations list (minimum info: ahps(station code), usgs_id)
ahps_list = 'AHPS_Potomac.csv'

# list with ahps station codes
station_list = pd.read_csv(f'{ahps_list}', dtype={'ahps':str})


######################################################################
## Main

# Function to retrieve AHPS data and return a Data Frame
def AHPS_data(gage):

    # Read HTML
    url = r'http://water.weather.gov/ahps2/hydrograph_to_xml.php?gage={}&output=tabular'.format(gage)
    r = requests.get(url)
    data = r.text
    soup = BeautifulSoup(data, "lxml")

    # Data
    data = soup.find_all('table')[0] 
    data_rows = data.find_all('tr')[3:]

    # Get the Current Year 
    year = datetime.now().strftime("%Y")

    # Initialize Dictionaries
    obs_data = {'datetime_utc' : [], 'stage_ft' : [], 'flow_cfs' : []}
    forecast_data = {'datetime_utc' : [], 'stage_ft' : [], 'flow_cfs' : []}

    # Extract values to Dictionaries
    value = 'Observed'

    for row in data_rows:
        d = row.find_all('td')
        try:
            dtm   = d[0].get_text().split()[0] + '/' + str(year) +' '+ d[0].get_text().split()[1]
            stage = d[1].get_text()
            flow  = d[2].get_text()
        
            if value == 'Observed':
                obs_data['datetime_utc'].append(dtm) 
                obs_data['stage_ft'].append(stage)
                obs_data['flow_cfs'].append(flow)
    
            elif value =='Forecast':
                forecast_data['datetime_utc'].append(dtm) 
                forecast_data['stage_ft'].append(stage)
                forecast_data['flow_cfs'].append(flow)
    
        except:
            check_value = str(d)
            if 'Forecast  Data ' in check_value:
                value = 'Forecast'

    # Create & Format Dataframes
    ## Forecast Data
    df = pd.DataFrame.from_dict(forecast_data)   
    df['datetime_utc'] = pd.to_datetime(df['datetime_utc'], format='%m/%d/%Y %H:%M')
    df['stage_ft'] = round(df['stage_ft'].astype(str).str[:-4].astype(np.float), 1)
    df['flow_cfs'] = round(df['flow_cfs'].astype(str).str[:-4].astype(np.float) * 1000, 1)

    return df


# Download data and save into a csv file
print(f'Retrieving AHPS forecast...')

df_ahps = pd.DataFrame()
j = 0

for gage in station_list.ahps:
    print(gage)
    df = AHPS_data(gage)
    if j == 0:
        df_ahps['datetime_utc'] = df.datetime_utc
        df_ahps[f'{gage}_stage_ft'] = df.stage_ft
        df_ahps[f'{gage}_flow_cfs'] = df.flow_cfs
        j = 1
    else:
        df_ahps[f'{gage}_stage_ft'] = df.stage_ft
        df_ahps[f'{gage}_flow_cfs'] = df.flow_cfs

date = df_ahps.loc[0, 'datetime_utc'].strftime('%Y%m%d%H')
df_ahps.to_csv(f'AHPS.{date}.csv', sep=',')

print(f'AHPS forecast starting at {date} completed!')