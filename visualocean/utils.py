from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import os
import datetime
import requests as r
import pandas as pd


def datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000)


def get_instrument_df(epochsec):
    req_inst = r.get('http://ooi.visualocean.net/instruments.json',
                     params={'_': epochsec})
    df = pd.DataFrame.from_records(req_inst.json()['data'])
    df.loc[:, 'status'] = df.apply(lambda x: x.status.split(' ')[-1:][0], axis=1)

    return df

def get_science_data_stream_meta(reference_designator, region):
    url = 'http://ooi.visualocean.net/data-streams/export'
    dsdf = pd.read_csv(os.path.join(url, region))

    return dsdf
