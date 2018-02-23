from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime
import json
import os
import time

import requests as r

from visualocean.urlbuilder import (create_data_url, create_meta_url)
from visualocean.utils import (datetime_to_string,
                               get_instrument_df,
                               get_nc_urls,
                               get_science_data_stream_meta,
                               unix_time_millis)

import xarray as xr


class OOIASSET(object):
    def __init__(self, site, node, sensor, method, stream):
        self.site = site
        self.node = node
        self.sensor = sensor
        self.method = method
        self.stream = stream
        self.thredds_url = None

    def __repr__(self):
        return '<OOIASSET: {}>'.format(
            '-'.join([self.site, self.node, self.sensor, self.method, self.stream]))

    def _get_data_url(self):
        return create_data_url(self.site, self.node, self.sensor, self.method, self.stream)

    def _get_metadata_url(self):
        return create_meta_url(self.site, self.node, self.sensor, self.method, self.stream)

    @staticmethod
    def _check_data_status(url):
        check_complete = os.path.join(url, 'status.txt')
        req = r.get(check_complete)
        while req.status_code != 200:
            req = r.get(check_complete)
            time.sleep(1)
        print('Request completed')  # noqa

    def request_data(self, begin_date, end_date=None, data_type='NetCDF', limit=None, credfile=''):
        """
        Function to request the data.
        It will take some time for NetCDF Data.
        Note that currently we only can get Science data.

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
            params['beginDT'] = begin_date

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

        data_url = self._get_data_url()
        with open(credfile, 'r') as f:
            creds = json.load(f)

        req = r.get(data_url,
                    auth=tuple(creds.values()),
                    params=params)

        # Checks and keeps trying if erroring out
        while req.status_code != 200:
            req = r.get(data_url,
                        auth=tuple(creds.values()),
                        params=params)

        data = req.json()
        try:
            with open(credfile, 'r') as f:
                creds = json.load(f)

            req = r.get(data_url,
                        auth=tuple(creds.values()),
                        params=params)
            data = req.json()

            self.thredds_url = data['allURLs'][0]
            print('Please wait while data is compiled.', end='')  # noqa
            print(data['allURLs'][1])  # noqa
            self._check_data_status(data['allURLs'][1])

            return self.thredds_url

        except Exception as e:
            print(e)  # noqa

    def request_metadata(self):
        return self._get_metadata_url()

    @classmethod
    def from_reference_designator(cls, reference_designator, method=None, stream=None):
        """

        :param reference_designator:
        :param method:
        :param stream:
        :return:
        """
        kw = {
            'method': method,
            'stream': stream
        }
        keys = ['site', 'node', 'sensor']
        val = reference_designator.split('-')
        values = val[:-2] + ['-'.join(val[-2:])]
        kw.update(dict(zip(keys, values)))

        # Checking for instrument status
        epochnow = unix_time_millis(datetime.datetime.utcnow())
        instdf = get_instrument_df(epochnow)
        desired_data = \
        instdf.loc[instdf['reference_designator'] == reference_designator].to_dict(orient='records')[
            0]  # noqa
        if desired_data['status'] != 'Operational':
            raise Exception('Current data is not operational!')

        # Getting method and stream if not available
        dsdf = get_science_data_stream_meta(reference_designator, desired_data['region'])
        if not method:
            method = dsdf.loc[dsdf['reference_designator'] == reference_designator]['method'].unique()[0]
            kw['method'] = method
        if not stream:
            stream = dsdf.loc[dsdf['reference_designator'] == reference_designator]['stream_name'].unique()[0]  # noqa
            kw['stream'] = stream

        return cls(**kw)

    def to_xarray(self, **kwargs):
        datasets = get_nc_urls(self.thredds_url)
        return xr.open_mfdataset(datasets, **kwargs)
