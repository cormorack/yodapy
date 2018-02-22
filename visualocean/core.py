from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import os
import requests as r
import time
import json
import datetime

from visualocean.utils import datetime_to_string
from visualocean.urlbuilder import create_data_url, create_meta_url


class OOIASSET(object):

    def __init__(self, site, node, sensor, method, stream):
        self.site = site
        self.node = node
        self.sensor = sensor
        self.method = method
        self.stream = stream

    def get_data_url(self):
        return create_data_url(self.site, self.node, self.sensor, self.method, self.stream)

    def get_metadata_url(self):
        return create_meta_url(self.site, self.node, self.sensor, self.method, self.stream)

    @staticmethod
    def _check_data_status(url):
        check_complete = os.path.join(url, 'status.txt')
        req = r.get(check_complete)
        print('Please wait while data is compiled.', end='')
        while req.status_code != 200:
            req = r.get(check_complete)
            time.sleep(1)
        print('Request completed')

    def request_data(self, begin_date, end_date=None, data_type='NetCDF', limit=None, credfile=''):
        """
        Function to request the data. It will take some time for NetCDF Data

        :param begin_date:
        :param end_date:
        :param data_type:
        :param limit:
        :param credfile:
        :return:
        """
        params = {
            'beginDT': begin_date
        }

        # Some checking for datetime and data_type
        if isinstance(begin_date, datetime.datetime):
            begin_date = datetime_to_string(begin_date)

        if end_date:
            if isinstance(end_date, datetime.datetime):
                end_date = datetime_to_string(end_date)
            params['endDT'] = end_date

        if data_type == 'JSON':
            if isinstance(limit, int):
                params['limit'] = limit
            else:
                raise Exception('Please enter limit for JSON data type. '
                                'Max limit is 20000 points.')

        data_url = self.get_data_url()
        try:
            with open(credfile, 'r') as f:
                creds = json.load(f)

            req = r.get(data_url, auth=tuple(creds.values()))
            data = req.json()

            thredds_url = data['allURLs'][0]
            print(data['allURLs'][1])
            self._check_data_status(data['allURLs'][1])

        except Exception as e:
            print(e)



    def request_metadata(self):
        return self.get_metadata_url()

    @classmethod
    def from_reference_designator(cls, reference_designator):
        val = reference_designator.split('-')
        values = val[:-2] + ['-'.join(val[-2:])]







