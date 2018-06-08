from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime

import pandas as pd

import xarray as xr

from yodapy.datasources import OOI


class TestOOIDataSource:

    def setup(self):
        self.region = ['cabled']
        self.site = ['axial base shallow profiler']
        # self.platform = 'Shallow Profiler'
        self.instrument = ['CTD']
        self.start_date = datetime.datetime(2018, 1, 1)
        self.end_date = datetime.datetime(2018, 2, 1)
        self._data_container = [pd.DataFrame.from_records([{
            'thredds_url': 'https://opendap.oceanobservatories.org/thredds'
                           '/catalog/ooi/landungs@uw.edu/20180606T232135'
                           '-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_optode_sample/catalog.html',  # noqa
            'status_url': 'https://opendap.oceanobservatories.org'
                          '/async_results/landungs@uw.edu/20180606T232135'
                          '-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_optode_sample'  # noqa
        }]), pd.DataFrame.from_records([{
            'thredds_url': 'https://opendap.oceanobservatories.org/thredds'
                           '/catalog/ooi/landungs@uw.edu/20180606T232135'
                           '-RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample/catalog.html',  # noqa
            'status_url': 'https://opendap.oceanobservatories.org'
                          '/async_results/landungs@uw.edu/20180606T232135'
                          '-RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample'  # noqa
        }])]

    def test_search_type(self):
        assert isinstance(self.region, (list, str))
        assert isinstance(self.site, (list, str))
        assert isinstance(self.instrument, (list, str))
        assert isinstance(self.start_date, datetime.datetime)
        assert isinstance(self.end_date, datetime.datetime)

    def test_search(self):
        search_results = OOI.search(region=self.region,
                                    site=self.site,
                                    instrument=self.instrument,
                                    begin_date=self.start_date,
                                    end_date=self.end_date)

        print(search_results.streams)
        assert isinstance(search_results, OOI)
        assert len(search_results) == 2

    def test_data_availibility(self):
        search_results = OOI.search(region=self.region,
                                    site=self.site,
                                    instrument=self.instrument,
                                    begin_date=self.start_date,
                                    end_date=self.end_date)

        assert isinstance(search_results.data_availibility(), type(None))

    def test_to_xarray(self):
        # TODO: Need smarter test in case OOI Server is down. Need caching of the sample netCDF!  # noqa
        ooi = OOI.search(region=self.region,
                         site=self.site,
                         instrument=self.instrument,
                         begin_date=self.start_date,
                         end_date=self.end_date)
        ooi._data_container = self._data_container

        dataset_list = ooi.to_xarray()
        assert isinstance(dataset_list, list)
        assert all(isinstance(data, xr.Dataset) for data in dataset_list)
