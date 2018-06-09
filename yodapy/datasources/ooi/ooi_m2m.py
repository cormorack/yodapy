# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)
import datetime
import re

from dask.distributed import (Client,
                              as_completed,
                              progress)
import pandas as pd
from requests import Session

from streamz import Stream
from streamz.dataframe import DataFrame

import xarray as xr

from yodapy.datasources.datasource import DataSource
from yodapy.datasources.ooi.m2m import MachineToMachine
from yodapy.datasources.ooi.helpers import (check_data_status,
                                            create_streams_cache)
from yodapy.datasources.ooi import SOURCE_NAME
from yodapy.utils.parser import (datetime_to_string,
                                 seconds_to_date,
                                 get_nc_urls)


class OOI(DataSource):
    def __init__(self, **kwargs):
        super(OOI, self).__init__()

        self._source_name = SOURCE_NAME
        self._start_date = kwargs.get('start_time')
        self._end_date = kwargs.get('end_time')

        self._streams = kwargs.get('streams')
        self._data_container = None
        self._data_type = 'netCDF'
        self._session = Session()

    def __repr__(self):
        object_repr = """{datasource}
        Number of streams: {stream_num}
        Streams range: {date_range}
        """.format

        date_string = 'Unknown'
        if not isinstance(self._start_date, type(pd.NaT)) and not isinstance(
                self._end_date, type(pd.NaT)):
            date_string = '{start} to {end}'.format(start='{:%Y-%m-%d}'.format(
                self._start_date), end='{:%Y-%m-%d}'.format(self._end_date))

        return object_repr(datasource=super().__repr__(),
                           stream_num=len(self._streams),
                           date_range=date_string)

    def __len__(self):
        return len(self._streams)

    @property
    def streams(self):
        return self._streams

    def _do_request(self, stream, params, **kwargs):
        m2m = MachineToMachine.use_existing_credentials()
        data = m2m.data_requests(session=self._session,
                                 stream=stream,
                                 params=params,
                                 **kwargs)

        if 'status_code' in data:
            if data['status_code'] != 200:
                raise Exception('{}'.format(data))

        if self._data_type == 'netCDF':
            return pd.DataFrame.from_records([{
                'thredds_url': data['allURLs'][0],
                'status_url': data['allURLs'][1]
            }])

        raw_pd = pd.DataFrame.from_records(data).copy()
        raw_pd.loc[:, 'time'] = raw_pd['time'].apply(
            lambda x: seconds_to_date(x))  # noqa
        return raw_pd

    def data_availibility(self):
        """
        Display the OOI Object Streams Data Availibility

        Returns:
            None

        """
        # TODO: Use M2M API to get the actual Data availability!
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        x = self._streams.long_display_name.values
        ends = self._streams.enddt.dt.to_pydatetime()
        starts = self._streams.startdt.dt.to_pydatetime()

        edate, bdate = [mdates.date2num(item) for item in (ends, starts)]

        ypos = range(len(edate))
        fig, ax = plt.subplots(figsize=(20, 10))
        ax.barh(ypos, edate - bdate,
                height=0.8, left=bdate, color='green',
                align='center')
        ax.set_title('OOI Data Availibility Graph')
        ax.set_yticks(ypos)
        ax.set_yticklabels(x)
        ax.xaxis_date()
        ax.axis('tight')

    @classmethod
    def search(cls, region=[], site=[], instrument=[],
               begin_date=None,
               end_date=None,
               stream_dataset='Science', **kwargs):
        """
        Search function to find desired data within OOI.

        Args:
            region (list): Name(s) of OOI Region.
            site (list): Name(s) of OOI Site.
            instrument (list): Name(s) of OOI Instrument
            begin_date (datetime): Desired data begin date.
            end_date (datetime): Desired data end date.
            stream_dataset (string): Type of stream dataset.
                                     Either 'Science' or 'Engineering'.
            **kwargs: Other keyword arguments.
                      To search by reference designator use
                      `reference_designator`.

        Returns:
            Pandas DataFrame of the desired data streams.

        """

        # Fix types
        if isinstance(region, str):
            region = [region]
        if isinstance(site, str):
            site = [site]
        if isinstance(instrument, str):
            instrument = [instrument]

        streams = create_streams_cache()
        df = None

        try:
            # Only search on available streams and initially just Science
            df = streams[~streams.stream.isnull() & streams.stream_dataset.str.match(stream_dataset)]  # noqa
        except Exception as e:
            print(e)

        availdf = df.copy()
        refd = kwargs.get('reference_designator')
        filtered = availdf[availdf.array_name.str.contains('|'.join(region),
                                                           flags=re.IGNORECASE) &  # noqa
                           availdf.site_name.str.contains('|'.join(site),
                                                          flags=re.IGNORECASE) &  # noqa
                           availdf.display_name.str.contains(
                               '|'.join(instrument),
                               flags=re.IGNORECASE)]
        if begin_date:
            if isinstance(begin_date, datetime.datetime):
                filtered = filtered[(begin_date >= availdf.startdt)]
            else:
                raise TypeError('Please provide datetime object for begin_date.')  # noqa
        if end_date:
            if isinstance(end_date, datetime.datetime):
                filtered = filtered[(end_date <= availdf.enddt)]
            else:
                raise TypeError('Please provide datetime object for end_date.')  # noqa
        if refd:
            filtered = availdf[availdf.reference_designator.str.contains(
                '|'.join(refd))]

        filtered_streams = filtered.reset_index(drop='index')
        return cls(streams=filtered_streams,
                   start_time=filtered_streams.startdt.min(),
                   end_time=filtered_streams.enddt.max())

    def request_data(self,
                     begin_date,
                     end_date=None,
                     data_type=None,
                     limit=None, **kwargs):
        """
        Function to request the data.
        It will take some time for NetCDF Data.

        Args:
            begin_date (datetime): Desired begin date of data.
            end_date (datetime): Desired end date of data.
            data_type (str): Desired data type. Currently `netCDF` by default.
            limit(int): Data point limit (for JSON `data_type`).

        Returns:

        """
        params = {}

        # Some checking for datetime and data_type
        if begin_date:
            if isinstance(begin_date, datetime.datetime):
                begin_date = datetime_to_string(begin_date)
                params['beginDT'] = begin_date

        if end_date:
            if isinstance(end_date, datetime.datetime):
                end_date = datetime_to_string(end_date)
                params['endDT'] = end_date

        if data_type:
            if data_type in ['netCDF', 'JSON']:
                self._data_type = data_type
            else:
                raise Exception('The data_type {} is not a valid, data_type. '
                                'Please use either netCDF or JSON.'.format(data_type))  # noqa

        if self._data_type == 'JSON':
            if isinstance(limit, int):
                params['limit'] = limit
                print('Requesting JSON...')
                print(params)
            else:
                raise Exception('Please enter limit for JSON data type. '
                                'Max limit is 20000 points.')
        elif self._data_type == 'netCDF':
            print('Please wait while data is compiled.\n')

        stream_list = list(map(lambda x: x[1],
                               self._streams.iterrows()))

        client = Client()
        futures = client.map(lambda st: self._do_request(st, params, **kwargs),
                             stream_list)
        progress(futures)

        data_urls = []
        for future, result in as_completed(futures, with_results=True):
            data_urls.append(result)

        self._data_container = data_urls

        return self

    def raw(self):
        """
        Retrieve the raw result when requesting data
        in a pandas DataFrame format.

        Returns:
            List of pandas DataFrame.
        """
        return self._data_container

    def to_xarray(self, **kwargs):
        """
        Retrieve the OOI streams data and export to Xarray Datasets.

        Args:
            **kwargs: Keyword arguments for xarray open_mfdataset.

        Returns:
            List of xarray datasets
        """
        dataset_list = None
        # TODO: What to do when it's JSON request, calling on to_xarray.
        # TODO: Standardize the structure of the netCDF to ensure CF compliance.  # noqa
        if self._data_type == 'netCDF':
            dataset_list = []
            client = Client()
            futures = client.map(lambda durl: check_data_status(
                self._session, durl
            ), self._data_container)
            progress(futures)

            for future, result in as_completed(futures, with_results=True):
                datasets = get_nc_urls(result)
                dataset_list.append(xr.open_mfdataset(datasets, **kwargs))

        return dataset_list

    def _stream_request(self, st_dict):
        st = st_dict['stream']
        params = st_dict['params']
        data = self._do_request(st,
                                params,
                                backoff_factor=0)
        return data

    @staticmethod
    def _update_params(dt, params):
        params['beginDT'] = datetime_to_string(dt)

    def stream_data(self, stream_name):
        # TODO: Figure out streaming all queried data
        # TODO: Check whether the data is available for streaming
        self._data_type = 'JSON'

        # get the current time
        end_date = datetime.datetime.utcnow()
        begin_date = end_date - datetime.timedelta(seconds=10)

        st = self._streams[self._streams.stream.str.match(stream_name)].iloc[0]

        params = dict()
        params['beginDT'] = datetime_to_string(begin_date)
        params['limit'] = 100

        sample_df = self._stream_request(st_dict={'stream': st,
                                                  'params': params})

        source = Stream()

        # Limited to request every 5s
        stream_source = source.map(self._stream_request).rate_limit(5.0)
        # Date updater
        stream_source.map(lambda x: x['time'].max().to_pydatetime()).sink(lambda x: self._update_params(x, params))  # noqa

        sdf = DataFrame(stream=stream_source, example=sample_df)
        sdf = sdf.set_index(['time'])

        from tornado import gen
        from tornado.ioloop import IOLoop

        async def f():
            while True:
                await gen.sleep(0.1)
                rec = {'stream': st,
                       'params': params}
                await source.emit(rec,
                                  asynchronous=True)

        IOLoop.current().add_callback(f)

        return sdf
