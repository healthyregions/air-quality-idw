# %%
import pandas as pd
import numpy as np
from datetime import datetime
from functools import reduce
from glob import glob
from os import path
from haversine import haversine

# %%
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def findOverlapping(row, df, threshold):
    lat = row['latitude']
    lon = row['longitude']
    dist = df[['latitude', 'longitude']].apply(lambda x: haversine((lat, lon), (x['latitude'], x['longitude'])), axis=1)
    return [id for id in df[dist < threshold]['id'].tolist() if id != row['id']]

def resolveId(row):
    if len(row['shared_id']) > 0 and row['shared_id'][0] < row['id']:
        return row['shared_id'][0]
    else: 
        return row['id']

def mergeIds(data):
    uniqLatLons = data[['latitude', 'longitude']].drop_duplicates()
    uniqLatLons['id'] = uniqLatLons.index
    uniqLatLons['shared_id'] = uniqLatLons.apply(lambda x: findOverlapping(x, uniqLatLons, .050), axis=1)
    uniqLatLons['id'] = uniqLatLons.apply(lambda x: resolveId(x), axis=1)
    uniqLatLons = uniqLatLons[['latitude','longitude','id']]
    uniqLatLons.columns = ['latitude','longitude','device_id']
    return uniqLatLons

def getData():
    temp_dir = path.join("temp", "raw_data.csv")
    data = pd.read_csv(temp_dir)[["msrDeviceNbr","longitude","latitude","readingDateTimeLocal","calibratedPM25"]]
    data = data[(data.calibratedPM25 > 0) & (data.calibratedPM25.notnull())]
    data['DATEOBJ'] = data['readingDateTimeLocal'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    data['IS_WEEKEND'] = data['DATEOBJ'].apply(lambda x: x.dayofweek >= 5)
    merged_ids = mergeIds(data)
    data = data.merge(merged_ids, on=['latitude', 'longitude'], how='left')
    return data.set_index('DATEOBJ')

def summarizeData(data, groupbyCol, analysisCol, prefix):
    summary = data.groupby(groupbyCol)[analysisCol].agg(["median", percentile(25), percentile(75), "std"]).reset_index()
    summary.columns = ['device_id', f'{prefix}_median', f'{prefix}_q25', f'{prefix}_q75', f'{prefix}_stddev']
    return summary

def main():
    data = getData()
    geo = data[['device_id', 'latitude', 'longitude']].drop_duplicates()
    aq = data[['device_id', 'calibratedPM25', 'IS_WEEKEND']]

    weekend = aq[aq.IS_WEEKEND]
    weekday = aq[~aq.IS_WEEKEND]
    rushHour = pd.concat([aq.between_time("8:00", "10:00"), aq.between_time("16:00", "18:00")])

    summary = summarizeData(aq,'device_id','calibratedPM25','topline')
    rushHour_summary = summarizeData(rushHour,'device_id','calibratedPM25','rushhour')
    weekend_summary = summarizeData(weekend,'device_id','calibratedPM25','weekend')
    weekday_summary = summarizeData(weekday,'device_id','calibratedPM25','weekday')

    dataframes = [geo, summary, rushHour_summary, weekend_summary, weekday_summary]
    return {
        "df": reduce(lambda left, right:pd.merge(left, right, on=['device_id'], how='inner'), dataframes),
        "timestamp": data.readingDateTimeLocal.max()
    }
                        
# %%
if __name__ == "__main__":
    data = main()
    df = data["df"]
    timestamp = data["timestamp"]
    print('Data summarized.')
    df.to_csv(path.join("temp", "data_summary.csv"), index=False)
# %%