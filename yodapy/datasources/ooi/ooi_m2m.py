# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)
import datetime
import re

import pandas as pd

from yodapy.datasources.datasource import DataSource
from yodapy.datasources.ooi.m2m import MachineToMachine
from yodapy.datasources.ooi.helpers import (STREAMS, extract_times)
from yodapy.datasources.ooi import SOURCE_NAME


class OOI(DataSource):
    def __init__(self, **kwargs):
        super(OOI, self).__init__()

        self._source_name = SOURCE_NAME

        self._streams = kwargs.get('streams', STREAMS)

        streams_range = extract_times()
        self._start_time = kwargs.get('start_time', streams_range[0])
        self._end_time = kwargs.get('end_time', streams_range[1])

    def __repr__(self):
        object_repr = """{datasource}
        Number of streams: {stream_num}
        Streams range: {date_range}
        """.format

        date_string = 'Unknown'
        if self._start_time and self._end_time:
            date_string = '{start} to {end}'.format(start='{:%Y-%m-%d}'.format(
                self._start_time), end='{:%Y-%m-%d}'.format(self._end_time))

        return object_repr(datasource=super().__repr__(),
                           stream_num=len(self._streams),
                           date_range=date_string)

    def __len__(self):
        return len(self._streams)

    @property
    def streams(self):
        return self._streams

    def data_availibility(self):
        """
        Display the OOI Object Streams Data Availibility

        Returns:
            None

        """
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

        plt.show()

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
                      To search by reference designator use `reference_designator`.

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
