import glob
import json
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from inflection import underscore

from db_manager import get_sql_engine

'''
  This script loads and normalizes raw analytics logs.
  Raw analytics logs should be placed in the `data` directory in the root of this repository.
'''

def convert_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
  '''
    Converts a column in the provided DataFrame from a string to a datetime object
    Can handle date strings in either of two formats:
      e.g. 'Sun Sep 27 2020 02:34:57 GMT+0000 (Coordinated Universal Time)' 
            or
            '2020-09-26 23:30:04.947+00'
  '''
  if df.iloc[0][column][0].isalpha():
    df[column] = df[column].map(lambda datestr: datetime.strptime(datestr.split(' GMT+0000 ', 1)[0], '%a %b %d %Y %H:%M:%S'))
  else:
    df[column] = df[column].map(lambda datestr: datetime.strptime(datestr.split('.', 1)[0].split('+', 1)[0], '%Y-%m-%d %H:%M:%S'))

def load_users_and_devices(users: pd.DataFrame) -> [pd.DataFrame, pd.DataFrame]:
  '''
    Converts a raw users data dump into two DataFrames:
      1. A users DataFrame, detailing individual users
      2. A devices DataFrame, detailing all devices used by the users (one-to-many relationship)
  '''
  users.drop(columns=['_id', 'appId', '__v', 'updatedAt', 'props.version'], inplace=True)
  user_info = users.filter(items=['userId', 'createdAt', 'props.country', 'props.locale'])
  user_info.columns = user_info.columns.str.replace('props.', '')
  user_info.rename(columns=lambda c: underscore(c), inplace=True)
  convert_to_datetime(user_info, 'created_at')

  # Users may have multiple devices
  # Device details are concatenated in a wide format
  # Split the device records into a normalized dataframe
  device_info = users.filter(regex='userId|devices.*(?<!osVersion)$')
  device_info.columns = device_info.columns.str.replace('devices\.\d\.', '')
  DEVICE_DATA_COLUMN_COUNT = 4  # Each device record has 4 data columns
  data_column_count = (len(device_info.columns) - 1)
  max_devices = data_column_count // DEVICE_DATA_COLUMN_COUNT
  all_device_info = None
  for i in range(max_devices):
    curr_data_columns = list(range(1 + (i * DEVICE_DATA_COLUMN_COUNT), ((i + 1) * DEVICE_DATA_COLUMN_COUNT) + 1))
    curr_device_info = device_info.iloc[:, [0] + curr_data_columns]
    curr_device_info = curr_device_info.dropna()
    if all_device_info is not None:
      all_device_info = all_device_info.append(curr_device_info)
    else:
      all_device_info = curr_device_info
  all_device_info.columns = all_device_info.columns.str.replace('^_id', 'device_id')
  all_device_info.rename(columns=lambda c: underscore(c), inplace=True)
  convert_to_datetime(all_device_info, 'last_seen')

  return [user_info, all_device_info]

def load_events(analytics: pd.DataFrame) -> [pd.DataFrame, pd.DataFrame]:
  '''
    Converts a raw analytics data dump into two DataFrames:
      1. A page_events DataFrame, detailing the pages viewed by individual users
      2. A action_events DataFrame, detailing specific actions that individual users completed
  '''
  analytics.rename(columns={'version':'app_version'}, inplace=True)
  analytics.drop(columns=['arch', 'avail_ram', 'country', 'duration', 'first_time', 'locale', 'module_version', 'os_version', 'platform', 'error_hash'], inplace=True)
  analytics.dropna(subset=['user_id'], inplace=True)
  convert_to_datetime(analytics, 'time')
  # Normalize the JSON column into column and concatenate the column on the dataframe
  analytics = (pd.concat({i: pd.json_normalize(json.loads(datum)) for i, datum in analytics.pop('data').items()})
          .reset_index(level=1, drop=True)
          .join(analytics)
          .reset_index(drop=True))

  is_screen_view_row = analytics.type.str.startswith('SCREEN_VIEW')
  page_events = analytics.loc[is_screen_view_row]
  page_events = page_events.drop(columns=['version', 'action', 'type', 'app_version'])
  action_events = analytics.loc[~is_screen_view_row]
  action_events = action_events.drop(columns=['version', 'screen'])

  return [action_events, page_events]

def load_data_directory():
  '''
    Loads analytics and user data dump CSV files from the data directory in this repository
    and parses them and inserts them into the database.
    Although there is only one analytics and users data file each, their names are suffixed
    with unique identifiers each time the data is exported.
  '''
  script_directory = os.path.dirname(__file__)
  raw_files = glob.glob(os.path.join(script_directory, '../../data/*.csv'))
  db_engine = get_sql_engine()
  for f in raw_files:
    file_name = os.path.basename(f)
    if file_name.startswith('users'):
      users = pd.read_csv(f)
      [users, devices] = load_users_and_devices(users)
      users.to_sql('users', db_engine, schema='public', if_exists='replace', index=False)
      devices.to_sql('devices', db_engine, schema='public', if_exists='replace', index=False)
    elif file_name.startswith('analytics'):
      analytics = pd.read_csv(f)
      [action_events, page_events] = load_events(analytics)
      action_events.to_sql('action_events', db_engine, schema='public', if_exists='replace', index=False)
      page_events.to_sql('page_events', db_engine, schema='public', if_exists='replace', index=False)

load_data_directory()
