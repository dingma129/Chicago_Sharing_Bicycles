
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
from datetime import datetime


# In[4]:

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
    trip_station['start_hour']=trip_station['starttime'].apply(lambda x:x.hour)
    trip_station['end_hour']=trip_station['stoptime'].apply(lambda x:x.hour)
    
    # extract the minute
    trip_station['start_minute']=trip_station['starttime'].apply(lambda x:x.minute)
    trip_station['end_minute']=trip_station['stoptime'].apply(lambda x:x.minute)
    
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
    columns = ['trip_id','year','month','day','dayofweek','weeknum', 'start_hour','start_minute','end_hour','end_minute', 'tripduration', 'usertype', 'gender', 'birthday', 'PRCP', 'SNOW', 'TMAX','TMIN','from_station_id', 'from_station_name','latitude_start', 'longitude_start', 'dpcapacity_start','to_station_id', 'to_station_name', 'latitude_end','longitude_end', 'dpcapacity_end']
    df=df[columns]
    
    return df