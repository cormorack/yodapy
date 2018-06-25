# -*- coding: utf-8 -*-

import os
import glob
import re
import sys
import logging
import datetime

import requests

from dask.distributed import (Client, as_completed, progress)
import pandas as pd

import xarray as xr

from yodapy.datasources.datasource import DataSource
from yodapy.datasources.ooi.m2m_client import M2MClient
from yodapy.utils.parser import get_nc_urls
from yodapy.utils.conn import requests_retry_session

SOURCE_NAME = 'OOI'


class OOI(DataSource):

    def __init__(self):
        super(OOI, self).__init__()

        self._source_name = SOURCE_NAME

        meta_pth = os.path.join(os.path.dirname(__file__), 'infrastructure')
        self._instruments = pd.read_csv(os.path.join(meta_pth, 'instruments.csv')).fillna('')  # noqa
        self._regions = pd.read_csv(os.path.join(meta_pth, 'regions.csv')).fillna('')
        self._sites = pd.read_csv(os.path.join(meta_pth, 'sites.csv')).fillna('')
        self._streams_descriptions = pd.read_csv(os.path.join(meta_pth, 'stream_descriptions.csv')).fillna('')  # noqa
        self._data_streams = pd.read_csv(os.path.join(meta_pth, 'data_streams.csv')).fillna('')  # noqa

        self._client = M2MClient()
        self._session = requests.session()
        self.username = self._client.api_username
        self.token = self._client.api_token

        self._filtered_instruments = self._instruments
        self._data_urls = None
        self._data_type = None

        self._logger = logging.getLogger(__name__)

    def __repr__(self):
        return f'<Data Source: {self._source_name}>'

    def __len__(self):
        return len(self._filtered_instruments)

    def view_instruments(self):
        return self._filtered_instruments

    def view_regions(self):
        return self._regions

    def view_sites(self):
        return self._sites

    def filter(self, region, site=None, instrument=None):
        """
        Filter for desired instruments by region, site, and instrument.

        Args:
            region:
            site:
            instrument:

        Returns:
            Dataframe

        """
        filtered_region = None
        filtered_sites = None
        filtered_instruments = self._instruments
        if region:
            region_search = list(map(lambda x: x.strip(' '), region.split(',')))  # noqa
            filtered_region = self._regions[self._regions.name.str.contains('|'.join(region_search), flags=re.IGNORECASE)]  # noqa

        if site:
            site_search = list(map(lambda x: x.strip(' '), site.split(',')))  # noqa
            filtered_sites = self._sites[self._sites.name.str.contains('|'.join(site_search), flags=re.IGNORECASE)]  # noqa
            if isinstance(filtered_region, pd.DataFrame):
                if len(filtered_region) > 0:
                    filtered_sites = filtered_sites[filtered_sites.reference_designator.str.contains('|'.join(filtered_region.reference_designator.values))]  # noqa

        if instrument:
            instrument_search = list(map(lambda x: x.strip(' '), instrument.split(',')))  # noqa
            filtered_instruments = self._instruments[self._instruments.name.str.contains('|'.join(instrument_search), flags=re.IGNORECASE)]  # noqa

        if isinstance(filtered_region, pd.DataFrame):
            if len(filtered_region) > 0:
                filtered_instruments = filtered_instruments[filtered_instruments.reference_designator.str.contains('|'.join(filtered_region.reference_designator.values))]  # noqa

        if isinstance(filtered_sites, pd.DataFrame):
            if len(filtered_region) > 0:
                filtered_instruments = filtered_instruments[filtered_instruments.reference_designator.str.contains('|'.join(filtered_sites.reference_designator.values))]  # noqa

        self._filtered_instruments = filtered_instruments  # noqa
        return self

    def clear(self):
        """
        Clears the filter

        Returns:

        """
        self._filtered_instruments = self._instruments
        return self

    def _retrieve_availibility(self, inst, stream_type='Science'):
        """
        Retrieves instrument streams availability.
        Args:
            inst (DataFrame): Instruments DataFrame.
            stream_type: Stream Type (Engineering, Science, all).

        Returns:

        """
        all_streams = {}
        for i, v in inst.iterrows():
            st = self._client.fetch_instrument_streams(v.reference_designator)
            if stream_type != 'all':
                st = list(filter(lambda s: self._client.fetch_stream_metadata(s['stream'])['stream_type']['value'] == stream_type, st))  # noqa
            if st:
                if v.preferred_stream:
                    filt_st = list(filter(lambda s: s['stream'] == v.preferred_stream, st))  # noqa
                    if filt_st:
                        all_streams[v.reference_designator] = filt_st
                    else:
                        self._logger.warning(f'{v.preferred_stream} NOT FOUND IN {v.reference_designator}. Available Streams: {st}')  # noqa
                else:
                    self._logger.warning(f'{v.reference_designator} DOES NOT HAVE PREFERRED STREAM!')  # noqa
            else:
                self._logger.warning(f'{v.reference_designator} does not have available streams')  # noqa

        return all_streams

    def _check_data_status(self, data, **kwargs):
        urls = {
            'thredds_url': data['allURLs'][0],
            'status_url': data['allURLs'][1]
        }
        check_complete = os.path.join(urls['status_url'], 'status.txt')

        req = None
        self._logger.debug(f"Your data ({urls['status_url']}) is still compiling... Please wait.")  # noqa
        while not req:
            req = requests_retry_session(session=self._session, **kwargs).get(
                check_complete)
        self._logger.debug('Request completed.')  # noqa

        return urls['thredds_url']

    def data_availability(self):
        """
        Plots data availably of provided instruments.

        Args:
            inst:

        Returns:

        """
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        inst = self._filtered_instruments
        if isinstance(inst, pd.DataFrame):
            if len(self._filtered_instruments) > 0:
                instruments_avail = self._retrieve_availibility(inst)
                x = list(map(
                    lambda rd: f"{inst.set_index('reference_designator').at[rd, 'name']} - {rd}",  # noqa
                    instruments_avail.keys()))
                ends = list(
                    map(lambda rd: pd.to_datetime(
                        rd[1][0]['endTime']).to_pydatetime(),
                        instruments_avail.items()))
                starts = list(map(
                    lambda rd: pd.to_datetime(
                        rd[1][0]['beginTime']).to_pydatetime(),
                    instruments_avail.items()))

                edate, bdate = [mdates.date2num(item) for item in
                                (ends, starts)]

                ypos = range(len(edate))
                fig, ax = plt.subplots(figsize=(20, 10))
                ax.barh(ypos, edate - bdate,
                        height=0.8, left=bdate, color='green',
                        align='center')
                ax.set_title('OOI Data Availibility Graph')
                ax.set_yticks(ypos)
                ax.set_yticklabels(x)
                ax.xaxis_date()
            else:
                self._logger.warning('Dataframe is empty...')
        else:
            self._logger.warning('Please find your desired instruments by using OOI().search() method.')  # noqa
            return None

    def request_data(self, begin_date=None, end_date=None,
                     data_type='netcdf', limit=-1, **kwargs):
        """
        Request data for instruments.

        Args:
            begin_date:
            end_date:
            data_type:
            limit:
            **kwargs:

        Returns:

        """
        instrument_avail = self._retrieve_availibility(self._filtered_instruments)
        request_urls = list(map(lambda inst: self._client.instrument_to_query(inst[0],
                                                                              user=self.username,
                                                                              stream=inst[1][0]['stream'],
                                                                              begin_ts=begin_date,
                                                                              end_ts=end_date,
                                                                              application_type=data_type,
                                                                              limit=limit,
                                                                              **kwargs)[0],
                                instrument_avail.items()))

        client = Client()
        futures = client.map(lambda url: self._client.send_request(url),
                             request_urls)
        progress(futures)

        data_urls = []
        for future, result in as_completed(futures, with_results=True):
            self._logger.debug(f'Requesting data: {future}')
            data_urls.append(result)

        self._data_urls = data_urls
        self._data_type = data_type.lower()
        return self

    def raw(self):
        return self._data_urls

    def to_xarray(self, **kwargs):
        """
        Retrieve the OOI streams data and export to Xarray Datasets.

        Args:
            **kwargs: Keyword arguments for xarray open_mfdataset.

        Returns:
            List of xarray datasets
        """
        from yodapy.datasources.ooi.helpers import check_data_status
        dataset_list = None
        # TODO: What to do when it's JSON request, calling on to_xarray.
        # TODO: Standardize the structure of the netCDF to ensure CF compliance.  # noqa
        if self._data_type == 'netcdf':
            dataset_list = []
            client = Client()
            futures = client.map(lambda durl: self._check_data_status(durl),
                                 self._data_urls)
            progress(futures)

            for future, result in as_completed(futures, with_results=True):
                self._logger.debug(f'Retrieving data: {future}')
                datasets = get_nc_urls(result)
                dataset_list.append(xr.open_mfdataset(datasets, **kwargs))

        return dataset_list
