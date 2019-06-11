from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import numpy as np
import datetime as dt

IMPALA_HOST = os.getenv('IMPALA_HOST', '<data lake>')
conn = connect(host=, port=, auth_mechanism='', use_ssl=True)
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
tables = as_pandas(cursor)
cursor.execute('select srchstname, dsthstname, time)
df = as_pandas(cursor)

#SET TIME AS INDEX'''
df.set_index('time', inplace=True)
df['tvalue'] = df.index

#SORT VALUES
df = df.sort_values(by=['srchstname','dsthstname','tvalue'])

#FIND THE DELTA
source_changed = df['srchstname'] != df['srchstname'].shift()
dest_changed = df['dsthstname'] != df['dsthstname'].shift()
change_occurred = (source_changed | dest_changed)
time_diff = df['tvalue'].diff()
now = dt.datetime.utcnow()
zero_delta = now - now
df['time_diff'] = time_diff
df['change_occurred'] = change_occurred

#REMOVE THE LAST 6 OF STRING
df['time_diff'] = df['time_diff'].astype(str)
df['time_diff']= df.apply(lambda x: x['time_diff'][:-6], axis = 1)

#SPLIT THE DATA
df[['Days', 'Time']] = df['time_diff'].str.split('days', n=1, expand=True)

#IF CHANGE == TRUE REPLACE WITH 00:00:00.000
df.loc[df['change_occurred'] == True, 'Time'] = '00:00:00.000'

#SELECT AND RENAME
df = df[['srchstname','dsthstname','Time']]
df.columns = [['srchstname','dsthstname','delta']]

#REMOVE THE MULTI-INDEX
df.columns = df.columns.get_level_values(0)
df = df.reset_index(drop=True)

#CONVERT DELTA BACK TO TIME
df['delta'] = pd.to_timedelta(df['delta'], unit='ms')

#SECONDS COLUMN STORED AS FLOAT
df['seconds'] = df['delta'].dt.total_seconds()

#USE THE MEDIAN TO FIND UPPER AND LOWER BOUNDS
df_median = df[['srchstname','dsthstname','seconds']].groupby(['srchstname','dsthstname']).median().reset_index()
df_median['bounds'] = df_median['seconds'] * .5
df_median['lower_bounds'] = df_median['seconds'] - df_median['bounds']
df_median['upper_bounds'] = df_median['seconds'] + df_median['bounds']
df_median = df_median[['srchstname','dsthstname','lower_bounds','upper_bounds']]

#ADD DF_MEDIAN DATA TO DF
df = df[['srchstname','dsthstname','seconds']]
df = pd.merge(df, df_median, how='inner', on=['srchstname', 'dsthstname'])

#ADD BOOLEAN TRUE OR FALSE IF MEETS CONDITION
df['fall_between'] = (df['seconds'] >= df['lower_bounds']) & (df['seconds'] <= df['upper_bounds'])

#NEW DATAFRAME
counts = df[['srchstname','dsthstname','fall_between']]
counts = pd.crosstab([counts.srchstname, counts.dsthstname], counts.fall_between).reset_index()
counts.columns.name = None

#ADD TOTAL COLUMN
counts.columns = ['srchstname','dsthstname','false','true']
counts['total'] = counts['false'] + counts['true']
counts['perc_true'] = counts['true'] / counts['total']

#ADD COUNTS TO DATAFRAME
counts = counts[['srchstname','dsthstname','perc_true','total']]
counts.columns = ['srchstname','dsthstname','perc_true','count']

#SORT VALUES ASCENDING
counts = counts.sort_values(['count','perc_true'], ascending=[False, False])
