# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os

from yodapy.utils.conn import requests_retry_session


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
