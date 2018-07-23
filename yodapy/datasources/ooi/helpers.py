# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import logging
import os

import numpy as np

from yodapy.utils.conn import requests_retry_session
from yodapy.utils.parser import seconds_to_date

logger = logging.getLogger(__name__)


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


def preprocess_ds(ds):
    cleaned_ds = ds.swap_dims({'obs': 'time'})
    logger.debug('DIMS SWAPPED')
    cleaned_ds['time'] = np.array(list(map(lambda x: seconds_to_date(x),
                                           cleaned_ds.time.values)))
    logger.debug('COMPLETE')
    return cleaned_ds
