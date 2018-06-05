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


STREAMS = create_streams_cache()


def extract_times():
    return STREAMS.startdt.min().to_pydatetime(), \
           STREAMS.enddt.max().to_pydatetime()
