# National Water Model (NWM) Outputs
# (Short, Medium, and Long Range)
# Extract streamflow forecast for selected reaches
# Convert streamflow from m3/s to ft3/s
# Author: Gustavo Coelho
# Jul, 2020


######################################################################
## Libraries
from datetime import datetime, timedelta
import netCDF4
import numpy as np
import pandas as pd
import urllib.request
import xarray as xr



######################################################################
## Functions

def truncate(n, decimals=0):
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier



######################################################################
## Settings

# List with NWM selected reaches
reaches_list = pd.read_csv('NWM_Potomac.csv')

# Forecast data 
date = (datetime.today() - timedelta(1)).strftime('%Y%m%d')
print(date)



######################################################################
##--SHORT RANGE FORECAST

for t in range (0,24,6):
    df_forecast = pd.DataFrame()  # New Data Frame for forecast starting at t hour
    t1 = '{:02d}'.format(t)
    print(f't{t1}.z')
    
    # Download NWM Short Range NetCDF files
    for f in range (1,19,1):
        f1 = '{:03d}'.format(f)
        print(f1)
        file_path = f'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{date}/short_range/nwm.t{t1}z.short_range.channel_rt.f{f1}.conus.nc'
        file_out = f'tmp/nwm.t{t1}z.short_range.channel_rt.f{f1}.conus.nc'     
        urllib.request.urlretrieve(file_path, file_out)

    # Open NWM files and extract streamflow for selected reach
    nc = xr.open_mfdataset(f'tmp/nwm.t{t1}z.short_range.channel_rt.f*.conus.nc', concat_dim='time', combine='nested')
    df = pd.DataFrame()

    for i in nc.time:
        j = pd.to_datetime(i.values)
        for nwm_reach_id in reaches_list.nwm_reach_id:
            df.loc[j, f'{nwm_reach_id}'] = np.around((nc.sel(time=i,feature_id=nwm_reach_id).streamflow)*35.3147,1)
            df.loc[j, f'{nwm_reach_id}'] = truncate(df.loc[j, f'{nwm_reach_id}'],1)
    df.index.names=['datetime_utc']
    
    # Save DataFrame into csv file
    df.to_csv(f'NWM.short_range.{date}{t1}.csv', sep=',')
    nc.close()
    
print('Short range completed!')



######################################################################
##--MEDIUM RANGE FORECAST

for t in range (0,24,6):
    df_forecast = pd.DataFrame()  # New Data Frame for forecast starting at t hour
    t1 = '{:02d}'.format(t)
    print(f't{t1}.z')
    
    # Download NWM Medium Range NetCDF files
    for f in range (3,241,3):
        f1 = '{:03d}'.format(f)
        print(f1)
        file_path = f'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{date}/medium_range_mem1/nwm.t{t1}z.medium_range.channel_rt_1.f{f1}.conus.nc'
        file_out = f'tmp/nwm.t{t1}z.medium_range.channel_rt_1.f{f1}.conus.nc'     
        urllib.request.urlretrieve(file_path, file_out)

    # Open NWM files and extract streamflow for selected reach
    nc = xr.open_mfdataset(f'tmp/nwm.t{t1}z.medium_range.channel_rt_1.f*.conus.nc', concat_dim='time', combine='nested')
    df = pd.DataFrame()

    for i in nc.time:
        j = pd.to_datetime(i.values)
        for nwm_reach_id in reaches_list.nwm_reach_id:
            df.loc[j, f'{nwm_reach_id}'] = np.around((nc.sel(time=i,feature_id=nwm_reach_id).streamflow)*35.3147,1)
            df.loc[j, f'{nwm_reach_id}'] = truncate(df.loc[j, f'{nwm_reach_id}'],1)
    df.index.names=['datetime_utc']
    
    # Save DataFrame into csv file
    df.to_csv(f'NWM.medium_range.{date}{t1}.csv', sep=',')
    nc.close()
    
print('Medium range completed!')



######################################################################
##--LONG RANGE FORECAST

for rt in range (1,5):
    print(f'rt_{rt}')

    for t in range (0,24,24):
        df_forecast = pd.DataFrame()  # New Data Frame for forecast starting at t hour
        t1 = '{:02d}'.format(t)
        print(f't{t1}.z')
    
        # Download NWM Long Range NetCDF files
        for f in range (6,721,6):
            f1 = '{:03d}'.format(f)
            print(f1)
            file_path = f'ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/nwm/prod/nwm.{date}/long_range_mem{rt}/nwm.t{t1}z.long_range.channel_rt_{rt}.f{f1}.conus.nc'
            file_out = f'tmp/nwm.t{t1}z.long_range.channel_rt_{rt}.f{f1}.conus.nc'     
            urllib.request.urlretrieve(file_path, file_out)       

        # Open NWM files and extract streamflow for selected reach
        nc = xr.open_mfdataset(f'tmp/nwm.t{t1}z.medium_range.channel_rt_1.f*.conus.nc', concat_dim='time', combine='nested')
        df = pd.DataFrame()

        for i in nc.time:
            j = pd.to_datetime(i.values)
            for nwm_reach_id in reaches_list.nwm_reach_id:
                df.loc[j, f'{nwm_reach_id}'] = np.around((nc.sel(time=i,feature_id=nwm_reach_id).streamflow)*35.3147,1)
                df.loc[j, f'{nwm_reach_id}'] = truncate(df.loc[j, f'{nwm_reach_id}'],1)
        df.index.names=['datetime_utc']
    
        # Save DataFrame into csv file
        df.to_csv(f'NWM.long_range_{rt}.{date}{t1}.csv', sep=',')
        nc.close()
    
print('Long range completed!')
