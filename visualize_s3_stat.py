"""
Takes in JSON file, metric name and sampling interval (in pandas-comliant format).
Uses plotly to chart the metric as a function of 'LastUpdated' timestamp.
"""

import json
import pandas as pd
import plotly.offline as py

if __name__ == '__main__':
    # Parsing S3 API response file
    with open('../s3listing.json') as in_file:
        s3_dat = json.load(in_file)
    # Only re-parsing the 'Contents' section of the API response into Pandas dataframe
    df = pd.read_json(
        json.dumps(s3_dat['Contents']), 
        orient='records', 
        convert_dates=['LastModified']
    )

    # Yielding stats dataframe ( hour by hour count of records - a.k.a count of files uploaded within the hour to S3 )
    stats_count = df.resample('1h', on='LastModified').count()
    # Prepping definition for plottly chart from the stats dataframe
    chart_freq = {
    'x': stats_count.index,
    'y': stats_count['Key'],
    'type': 'bar'
    }
    # Producing visualization to html file
    py.plot({'data': [chart_freq]}, filename='upload-freq-hour-by-hour.html')

    # Yielding stats dataframe ( hour by hour total size of data uploaded within each hour to S3 )
    stats_size = df.resample('1h', on='LastModified').sum()
    # Prepping definition for plotly chart from the stats dataframe
    chart_size = {
    'x': stats_size.index,
    'y': stats_size['Size'],
    'type': 'bar'
    }
    # Producing visualization to html file
    py.plot({'data': [chart_size]}, filename='upload-volume-hour-by-hour.html')