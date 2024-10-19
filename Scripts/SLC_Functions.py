import os
import pandas as pd
import numpy as np




"""
Generates a metadata summary for water level data files, including 
station ID, date range, and water level statistics (mean, median, min, max).

Args:
    datapath (str): Path to the directory containing the CSV files.

Returns:
    pd.DataFrame: A DataFrame containing the metadata for each station.
"""

def create_metadata_from_SLC_Data(datapath: str) -> pd.DataFrame:

    station_id = []
    datemin = []
    datemax = []
    datamean = []
    datamedian = []
    datamax = []
    datamin = []

    for file in os.listdir(datapath):
        data = pd.read_csv(os.path.join(datapath, file), parse_dates=['date'])
        data = data.replace(-66577, np.nan)
        
        station_id.append(file.split(".")[0])
        datemin.append(data['date'].min())
        datemax.append(data['date'].max())
        datamean.append(data['data'].mean())
        datamedian.append(data['data'].median())
        datamin.append(data['data'].min())
        datamax.append(data['data'].max())
    
    data_describe = pd.DataFrame({
        'station_id': station_id, 
        'date_min': datemin,
        'date_max': datemax,
        'mean_WL': datamean,
        'median_WL': datamedian,
        'min_WL': datamin,
        'max_WL': datamax
    })
    
    return data_describe





"""
    Reads CSV files from the directory, cleans the data, removes outliers, 
    converts values, and resamples to daily frequency. Returns a dictionary 
    with filenames as keys and processed DataFrames as values.
     Args:
        directory (str): Path to the directory containing CSV files.
        std_threshold (int): Number of standard deviations for outlier removal (default is 3).

    """

def process_SLC_Data_To_daily(directory, std_threshold: int = 3) -> dict:
    daily_dic = {}
    
    # Iterate through the files in the given directory
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        
        # Read the CSV file, parse 'date' column as dates
        data = pd.read_csv(file_path, parse_dates=['date'])
        
        # Replace invalid values with NaN
        data = data.replace(-66577, np.nan)
        
        # Remove outliers beyond the given number of standard deviations
        data = data[((data['data'] - data['data'].mean()) / data['data'].std()).abs() < std_threshold]
        
        # Convert from decimal feet to feet
        data['data'] = data['data'] / 100
        
        # Set to Hawaii UTC-10 timezone
        data['date'] = pd.to_datetime(data['date'], utc=True) # Ensure the date column is in datetime format and set to UTC
        # Convert to Hawaii Standard Time (HST, UTC-10)
        data['date']  = data['date'] .dt.tz_convert('Etc/GMT+10')
        
        # Set the 'date' column as index and resample to daily frequency
        data.set_index('date', inplace=True)
        data_daily = data.resample('D').mean()
        
        # Store the resampled daily data in the dictionary
        daily_dic[file] = data_daily
    
    return daily_dic


''' Same for the USGS data '''

def process_USGS_Data_To_daily(USGS_directory, std_threshold: int = 3) -> dict:
    daily_dic_USGS = {}

    # Iterate through the files in the given directory
    for file in os.listdir(USGS_directory):
        file_path = os.path.join(USGS_directory, file)

        # Read the CSV file, parse 'date' column as dates
        data = pd.read_csv(file_path, parse_dates=['dateTime'])

        # Standardize cols to UHSLC names 
        data.rename(columns={'dateTime': 'date'}, inplace=True)  # standardize the date 
        data.rename(columns={'value': 'data'}, inplace=True)  # standardize the data 

        # Replace invalid values with NaN
        #data = data.replace(-66577, np.nan)   #### If there are any no datavalues???? 


        # Remove outliers beyond the given number of standard deviations 
        # Doesnt seem to be needed 
        #data = data[((data['data'] - data['data'].mean()) / data['data'].std()).abs() < std_threshold]


       # Note USGS data is already in Hawaii Timezone 

        # Set the 'date' column as index and resample to daily frequency
        data.set_index('date', inplace=True)
        data_daily = data.resample('D').mean()

        # Store the resampled daily data in the dictionary
        daily_dic_USGS[file] = data_daily
        
    return daily_dic_USGS