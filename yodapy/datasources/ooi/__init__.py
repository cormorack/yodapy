# -*- coding: utf-8 -*-

import logging
import os
import re

from dask.distributed import (Client, as_completed, progress)

import gevent
import grequests

import pandas as pd

import requests

import xarray as xr

from yodapy.datasources.datasource import DataSource
from yodapy.datasources.ooi.helpers import preprocess_ds
from yodapy.datasources.ooi.m2m_client import M2MClient
from yodapy.utils.parser import get_nc_urls

SOURCE_NAME = 'OOI'


class OOI(DataSource):
    """OOI Object for Ocean Observatories Initiative Data Retrieval.

    Attributes:
        username (str): Username for OOI API Data Access.
        token (str): Token for OOI API Data Access.
        source_name (str): Data source name.

    """

    def __init__(self):
        super(OOI, self).__init__()

        self._source_name = SOURCE_NAME

        meta_pth = os.path.join(os.path.dirname(__file__), 'infrastructure')
        self._instruments = pd.read_csv(os.path.join(meta_pth, 'instruments.csv')).fillna('')  # noqa
        self._regions = pd.read_csv(os.path.join(meta_pth, 'regions.csv')).fillna('')  # noqa
        self._sites = pd.read_csv(os.path.join(meta_pth, 'sites.csv')).fillna('')  # noqa
        self._streams_descriptions = pd.read_csv(os.path.join(meta_pth, 'stream_descriptions.csv')).fillna('')  # noqa
        self._data_streams = pd.read_csv(os.path.join(meta_pth, 'data_streams.csv')).fillna('')  # noqa

        self._client = M2MClient()
        self._session = requests.session()
        self.username = self._client.api_username
        self.token = self._client.api_token
        self.last_request_urls = None

        self._filtered_instruments = self._instruments
        self._data_urls = None
        self._data_type = None

        self._logger = logging.getLogger(__name__)

    def __repr__(self):
        return f'<Data Source: {self._source_name}>'

    def __len__(self):
        return len(self._filtered_instruments)

    def view_instruments(self):
        """
        Shows the current instruments requested.

        Returns:
            DataFrame: Pandas dataframe of the instruments.

        """
        return self._filtered_instruments

    def view_regions(self):
        """
        Shows the regions within OOI.

        Returns:
            DataFrame: Pandas dataframe of the regions.

        """
        return self._regions

    def view_sites(self):
        """
        Shows the sites within OOI.

        Returns:
            DataFrame: Pandas dataframe of the sites.

        """
        return self._sites

    def search(self, region, site=None, node=None, instrument=None):
        """
        Search for desired instruments by region, site, and/or instrument.

        Args:
            region (str): **Required** Region name. If multiple use comma separated.
            site (str): Site name. If multiple use comma separated.
            node (str): Node name. If multiple use comma separated.
            instrument (str): Instrument name. If multiple use comma separated.

        Returns:
            self: Modified OOI Object

        """
        filtered_region = None
        filtered_sites = None
        filtered_instruments = self._instruments
        if region:
            region_search = list(map(lambda x: x.strip(' '), region.split(',')))  # noqa
            filtered_region = self._regions[self._regions.name.str.contains('|'.join(region_search), flags=re.IGNORECASE) | self._regions.reference_designator.str.contains('|'.join(region_search), flags=re.IGNORECASE)]  # noqa

        if site:
            site_search = list(map(lambda x: x.strip(' '), site.split(',')))  # noqa
            filtered_sites = self._sites[self._sites.name.str.contains('|'.join(site_search), flags=re.IGNORECASE) | self._sites.reference_designator.str.contains('|'.join(site_search), flags=re.IGNORECASE)]  # noqa
            if isinstance(filtered_region, pd.DataFrame):
                if len(filtered_region) > 0:
                    filtered_sites = filtered_sites[filtered_sites.reference_designator.str.contains('|'.join(filtered_region.reference_designator.values))]  # noqa

        if instrument:
            instrument_search = list(map(lambda x: x.strip(' '), instrument.split(',')))  # noqa
            filtered_instruments = self._instruments[self._instruments.name.str.contains('|'.join(instrument_search), flags=re.IGNORECASE) | self._instruments.reference_designator.str.contains('|'.join(instrument_search), flags=re.IGNORECASE)]  # noqa

        if isinstance(filtered_region, pd.DataFrame):
            if len(filtered_region) > 0:
                filtered_instruments = filtered_instruments[filtered_instruments.reference_designator.str.contains('|'.join(filtered_region.reference_designator.values))]  # noqa

        if isinstance(filtered_sites, pd.DataFrame):
            if len(filtered_region) > 0:
                filtered_instruments = filtered_instruments[filtered_instruments.reference_designator.str.contains('|'.join(filtered_sites.reference_designator.values))]  # noqa

        if node:
            node_search = list(map(lambda x: x.strip(' '), node.split(',')))
            filtered_instruments = filtered_instruments[filtered_instruments.reference_designator.str.contains('|'.join(node_search), flags=re.IGNORECASE) | filtered_instruments.location.str.contains('|'.join(node_search), flags=re.IGNORECASE)]  # noqa

        self._filtered_instruments = filtered_instruments  # noqa
        return self

    def clear(self):
        """
        Clears the instrument filter.

        Returns:
            self: Modified OOI Object
        """
        self._filtered_instruments = self._instruments
        return self

    def _retrieve_availibility(self, inst, stream_type='Science'):
        """
        Retrieves instrument streams availability.
        Args:
            inst (DataFrame): Instruments DataFrame.
            stream_type (str): Stream Type (Engineering, Science, all).

        Returns:
            dict: All the streams for instruments.
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
        try:
            urls = {
                'thredds_url': data['allURLs'][0],
                'status_url': data['allURLs'][1]
            }
        except Exception as e:
            self._logger.info(e)
            return None
        check_complete = '/'.join([urls['status_url'], 'status.txt'])

        req = requests.get(check_complete)

        if req.status_code != 200:
            req = requests.get(check_complete)
            self._logger.info(
                f"Your data ({urls['status_url']}) is still compiling... Please wait.")  # noqa
            return None

        self._logger.info('Request completed.')  # noqa
        return urls['thredds_url']

    def data_availability(self):
        """
        Plots data availability of desired instruments.

        Returns:
            dict: Instruments availability dictionary and prints out matplotlib plot of the data availibility.

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

                return instruments_avail
            else:
                self._logger.warning('Dataframe is empty...')
        else:
            self._logger.warning('Please find your desired instruments by using OOI().search() method.')  # noqa
            return None

    def request_data(self, begin_date=None, end_date=None,
                     data_type='netcdf', limit=-1, stream=None, **kwargs):
        """
        Request data for filtered instruments.

        Args:
            begin_date (str, optional): Begin date of desired data in ISO-8601 Format.
            end_date (str, optional): End date of desired data in ISO-8601 Format.
            data_type (str): Desired data type. Either 'netcdf' or 'json'.
            limit (int, optional): Desired data points. Required for 'json' ``data_type``. Max is 20000.
            stream (str, optional): Reference designator of stream.
            **kwargs: Optional Keyword arguments. \n
                **telemetry** - telemetry type (Default is all telemetry types) \n
                **time_delta_type** - Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta' \n
                **time_delta_value** - Positive integer value to subtract from the end time to get the start time for subsetting. \n
                **time_check** - set to true (default) to ensure the request times fall within the stream data availability \n
                **exec_dpa** - boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters (Default is True) \n
                **provenance** - boolean value specifying whether provenance information should be included in the data set (Default is True) \n

        Returns:
            self: Modified OOI Object. Use ``raw()`` to see either data url for netcdf or json result for json.

        """
        if len(self._filtered_instruments) > 5:
            text = f'Too many instruments to request data for! Max is 5, you have {len(self._filtered_instruments)}'  # noqa
            self._logger.error(text)
            raise Exception(text)
        instrument_avail = self._retrieve_availibility(self._filtered_instruments)
        do_filter = instrument_avail.items()
        if stream:
            stream_list = list(map(lambda x: x.strip(' '), stream.split(',')))
            do_filter = filter(lambda inst: inst[1][0]['stream'] in stream_list, instrument_avail.items())

        try:
            request_urls = list(map(lambda inst: self._client.instrument_to_query(inst[0],
                                                                                  user=self.username,
                                                                                  stream=inst[1][0]['stream'],
                                                                                  begin_ts=begin_date,
                                                                                  end_ts=end_date,
                                                                                  application_type=data_type,
                                                                                  limit=limit,
                                                                                  **kwargs)[0],
                                    do_filter))
        except Exception as e:
            self._logger.warning(e)
            request_urls = []
        self.last_request_urls = request_urls

        reqs = (grequests.get(
                    url,
                    auth=(self._client.api_username,
                          self._client.api_token),
                    timeout=self._client.timeout,
                    verify=False) for url in request_urls
                )

        def exception_handler(request, exception):
            self._logger.error(exception)

        results = grequests.map(reqs,
                                exception_handler=exception_handler)

        data_urls = []
        try:
            data_urls = [r.json() for r in results]
        except Exception as e:
            self._logger.warning(e)

        self._data_urls = data_urls
        self._data_type = data_type.lower()
        return self

    def raw(self):
        return self._data_urls

    def check_status(self):
        turls = []
        for durl in self._data_urls:
            turl = self._check_data_status(durl)
            if turl:
                turls.append(turl)

        if len(turls) == len(self._data_urls):
            self._logger.warning('Request Completed')
            return (0, turls)
        return (-1, turls)

    def _fetch_xr(self, turl, **kwargs):
        datasets = get_nc_urls(turl)
        return xr.open_mfdataset(
            datasets,
            preprocess=preprocess_ds,
            decode_times=False,
            **kwargs)

    def to_xarray(self, **kwargs):
        """
        Retrieve the OOI streams data and export to Xarray Datasets.

        Args:
            **kwargs: Keyword arguments for xarray open_mfdataset.

        Returns:
            list: List of xarray datasets
        """
        dataset_list = []
        # TODO: What to do when it's JSON request, calling on to_xarray.
        # TODO: Standardize the structure of the netCDF to ensure CF compliance.
        # TODO: Add way to specify instruments to convert to xarray
        if self._data_type == 'netcdf':
            status = -1
            while status < 0:
                status, turls = self.check_status()
            if len(turls) > 0:
                jobs = [gevent.spawn(self._fetch_xr, url, **kwargs) for url in turls]
                gevent.joinall(jobs, timeout=300)
                dataset_list = [job.value for job in jobs]

            # client = Client()
            # self._logger.debug(f'to_xarray dask client: {client}')
            # futures = client.map(lambda durl: self._check_data_status(durl),
            #                      self._data_urls)
            # progress(futures)

            # for future, result in as_completed(futures, with_results=True):
            #     self._logger.debug(f'Retrieving data: {future}')
            #     if result:
            #         datasets = get_nc_urls(result)
            #         dataset_list.append(xr.open_mfdataset(
            #             datasets,
            #             preprocess=preprocess_ds,
            #             decode_times=False,
            #             **kwargs)
            #         )
        else:
            self._logger.warning(f'{self._data_type} cannot be converted to xarray dataset')  # noqa

        return dataset_list
