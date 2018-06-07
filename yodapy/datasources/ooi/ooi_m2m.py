# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)
import datetime
import re

from dask.distributed import (Client, as_completed)
import pandas as pd
from requests import Session
import xarray as xr

from yodapy.datasources.datasource import DataSource
from yodapy.datasources.ooi.m2m import MachineToMachine
from yodapy.datasources.ooi.helpers import (STREAMS,
                                            extract_times,
                                            check_data_status)
from yodapy.datasources.ooi import SOURCE_NAME
from yodapy.utils.parser import (datetime_to_string,
                                 get_nc_urls)


class OOI(DataSource):
    def __init__(self, **kwargs):
        super(OOI, self).__init__()

        self._source_name = SOURCE_NAME

        self._streams = kwargs.get('streams', STREAMS)

        streams_range = extract_times()
        self._start_date = kwargs.get('start_time', streams_range[0])
        self._end_date = kwargs.get('end_time', streams_range[1])

        self._data_urls = kwargs.get('data_urls')
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

    def _do_request(self, st_dict):
        m2m = MachineToMachine.use_existing_credentials()
        return m2m.data_requests(session=self._session, stream=st_dict['stream'],
                                 params=st_dict[
                                     'params'])

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
               begin_date=datetime.datetime(2000, 1, 1),
               end_date=datetime.datetime.utcnow(),
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

        try:
            # Only search on available streams and initially just Science
            df = STREAMS[~STREAMS.stream.isnull() &
                              STREAMS.stream_dataset.str.match(
                                  stream_dataset)]
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
                               flags=re.IGNORECASE) &
                           (begin_date >= availdf.startdt) &
                           (end_date <= availdf.enddt)]
        if refd:
            filtered = availdf[availdf.reference_designator.str.contains(
                '|'.join(refd))]

        filtered_streams = filtered.reset_index(drop='index')
        return cls(streams=filtered_streams,
                   start_time=filtered_streams.startdt.min(),
                   end_time=filtered_streams.enddt.max())

    def request_data(self,
                     begin_date=None,
                     end_date=None,
                     data_type='netCDF',
                     limit=None):
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
        params = {
            'beginDT': datetime_to_string(self._start_date),
            'endDT': datetime_to_string(self._end_date)
        }

        # Some checking for datetime and data_type
        if begin_date:
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

        stream_list = list(map(lambda x: {'stream': x[1], 'params': params},
                               self.streams.iterrows()))

        client = Client()
        futures = client.map(self._do_request, stream_list)

        data_urls = []
        for future, result in as_completed(futures, with_results=True):
            data_urls.append(result)

        print('Please wait while data is compiled.')  # noqa
        self._data_urls = data_urls
        return self

    def to_xarray(self, **kwargs):
        dataset_list = []
        client = Client()
        futures = client.map(lambda durl: check_data_status(
            self._session, durl
        ), self._data_urls)

        for future, result in as_completed(futures, with_results=True):
            datasets = get_nc_urls(result)
            dataset_list.append(xr.open_mfdataset(datasets, **kwargs))

        return dataset_list
