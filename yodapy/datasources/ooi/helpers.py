# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
import json

import requests
import pandas as pd

from yodapy.utils.meta import meta_cache
from yodapy.utils.conn import requests_retry_session
from yodapy.datasources.ooi import SOURCE_NAME


@meta_cache(SOURCE_NAME)
def create_streams_cache(fold_path):
    stream_cache = os.path.join(fold_path, 'streams.json')
    try:
        streams_json = requests.get(
            'https://ooinet.oceanobservatories.org/api/uframe/stream').json()
        with open(stream_cache, 'w') as f:
            f.write(json.dumps(streams_json))
    except Exception:
        print('Source data currently not available. Reading from cache...')
        with open(stream_cache, 'r') as f:
            streams_json = json.load(f)

    rawdf = pd.DataFrame.from_records(streams_json['streams']).copy()

    rawdf.loc[:, 'startdt'] = rawdf['start'].apply(
        lambda x: pd.to_datetime(x))
    rawdf.loc[:, 'enddt'] = rawdf['end'].apply(
        lambda x: pd.to_datetime(x))

    return rawdf


def check_data_status(session, data, **kwargs):
    urls = {
        'thredds_url': data['allURLs'][0],
        'status_url': data['allURLs'][1]
    }
    check_complete = os.path.join(urls['status_url'], 'status.txt')

    req = None
    print('\nYour data ({}) is still compiling... Please wait.'.format(
        os.path.basename(urls['status_url'])))
    while not req:
        req = requests_retry_session(session=session, **kwargs).get(
            check_complete)
    print('\nRequest completed.')  # noqa

    return urls['thredds_url']
