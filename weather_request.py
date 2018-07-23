
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import calendar
import time
import json


# In[2]:


def query_prcp(key, year):
    """ Qeury the prcp/snow information from https://www.ncdc.noaa.gov/
        in the given year
    """
    # lists for data and error
    df=[]
    errors=[]
    
    # url for precipitation
    url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid=GHCND:US1ILCK0036&units=metric&limit=1000'
    for day in [('-01-01','-06-30'),('-07-01','-12-31')]:
        
        # set start/end date every half year due to the limit of request
        start_date = str(year)+day[0]
        end_date = str(year)+day[1]
        
        # put the dates on url
        link=url+'&startdate='+start_date+'&enddate='+end_date
        
        try:
            # get .json
            data = requests.get(link,headers={'Token': key}).json()['results']
            time.sleep(6)
            
            # switch into a pandas.DataFrame
            df.append(pd.DataFrame(data).pivot_table(values='value', index='date', columns='datatype', aggfunc='mean'))
        except:
            errors.append((year,day))            
    # returns the concatenation of every half year and error
    return pd.concat(df), errors


# In[3]:


def query_temp(key, year):
    """ Qeury the temperature information from https://www.ncdc.noaa.gov/
        in the given year
    """
    # lists for data and error
    df=[]
    errors=[]
    
    # url for temperature
    url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&stationid=GHCND:USC00111550&units=metric&limit=1000'
    for day in [('-01-01','-06-30'),('-07-01','-12-31')]:
        
        # set start/end date every half year due to the limit of request
        start_date = str(year)+day[0]
        end_date = str(year)+day[1]
        
        # put the dates on url
        link=url+'&startdate='+start_date+'&enddate='+end_date
        
        try:
            # get .json
            data = requests.get(link,headers={'Token': key}).json()['results']
            time.sleep(6)
            
            # switch into a pandas.DataFrame
            df.append(pd.DataFrame(data).pivot_table(values='value', index='date', columns='datatype', aggfunc='mean'))
        except:
            errors.append((year,day))            
    # returns the concatenation of every half year and error
    return pd.concat(df), errors

def merge_data(prcp,temp,trip,station):
    '''
    Merge prcp, temp, trip, station dataframes
    '''
    # combine prcp df and temp df to a weather df
    weather = pd.merge(left=prcp, right=temp, how='outer', left_on='date', right_on='date')
    weather.drop(columns='TOBS',inplace=True)
    
    # combine station df into trip df for both from_ and to_
    # put suffixes _start and _end for corresponding case
    trip_start = pd.merge(left=trip, right=station, how='left', left_on='from_station_id', right_on='id')
    trip_station = pd.merge(left=trip_start, right=station, how='left',left_on='to_station_id', right_on='id',                        suffixes=('_start', '_end'))
    
    # extract the date (to be used when merge with weather df)
    trip_station['date']=trip_station['starttime'].apply(lambda x:datetime(x.year,x.month,x.day))
    
    # extract the hour
    trip_station['hour']=trip_station['starttime'].apply(lambda x:x.hour)
    
    # extract the day of the week (0 for Sunday)
    trip_station['dayofweek']=trip_station['starttime'].apply(lambda x:x.isoweekday())
    
    # extract the number of the week (0 for the first week for each year)
    trip_station['weeknum']=trip_station['starttime'].apply(lambda x:x.strftime("%U"))
    
    # combine trip_station with weather
    df = pd.merge(left=trip_station, right=weather, how='left', left_on='date', right_on='date')
    
    # split date into 3 columns year, month, day
    df['year']=df['date'].apply(lambda x:x.strftime("%Y"))
    df['month']=df['date'].apply(lambda x:x.strftime("%m"))
    df['day']=df['date'].apply(lambda x:x.strftime("%d"))
    
    # columns want to be kept in the final df
    columns = ['trip_id','year','month','day','hour','dayofweek','weeknum', 'usertype', 'gender', 'birthyear', 'starttime', 'stoptime','tripduration',           'from_station_id', 'from_station_name','latitude_start', 'longitude_start', 'dpcapacity_start',           'to_station_id', 'to_station_name', 'latitude_end','longitude_end', 'dpcapacity_end']
    df=df[columns]
    
    return df
