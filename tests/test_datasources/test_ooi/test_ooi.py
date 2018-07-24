from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime

import pandas as pd
import pytest

import xarray as xr

from yodapy.datasources import OOI


class TestOOIDataSource:

    def setup(self):
        self.OOI = OOI()
        self.region = 'cabled array'
        self.site = 'axial base shallow profiler'
        # self.platform = 'Shallow Profiler'
        self.instrument = 'CTD'
        self.start_date = '2018-06-01'
        self.end_date = '2018-06-02'
        self._data_urls = [{'requestUUID': '609c7970-8065-46fa-9fd3-0975c97a1f28',
          'outputURL': 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180625T215711-RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample/catalog.html',
          'allURLs': ['https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180625T215711-RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample/catalog.html',
           'https://opendap.oceanobservatories.org/async_results/landungs@uw.edu/20180625T215711-RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample'],
          'sizeCalculation': 5548554,
          'timeCalculation': 60,
          'numberOfSubJobs': 2},
         {'requestUUID': 'd842b42a-c231-47ec-a015-0fb68a91b7cc',
          'outputURL': 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180625T215711-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_optode_sample/catalog.html',
          'allURLs': ['https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180625T215711-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_optode_sample/catalog.html',
           'https://opendap.oceanobservatories.org/async_results/landungs@uw.edu/20180625T215711-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_optode_sample'],
          'sizeCalculation': 5595265,
          'timeCalculation': 60,
          'numberOfSubJobs': 1}]

    def test_searc(self):
        search_results = self.OOI.searc(region=self.region,
                                         site=self.site,
                                         instrument=self.instrument)

        assert isinstance(search_results, OOI)
        assert len(search_results) == 2

    @pytest.mark.skip(reason='Need credentials.')
    def test_view_instruments(self):
        inst = self.OOI.view_instruments()

        assert isinstance(inst, pd.DataFrame)

    @pytest.mark.skip(reason='Need credentials.')
    def test_view_regions(self):
        inst = self.OOI.view_regions()

        assert isinstance(inst, pd.DataFrame)

    @pytest.mark.skip(reason='Need credentials.')
    def test_view_instruments(self):
        inst = self.OOI.view_sites()

        assert isinstance(inst, pd.DataFrame)

    @pytest.mark.skip(reason='Need credentials.')
    def test_data_availibility(self):
        search_results = self.OOI.search(region=self.region,
                                         site=self.site,
                                         instrument=self.instrument)

        assert isinstance(search_results.data_availibility(), type(None))

    @pytest.mark.skip(reason='Inconsistent connection to data.')
    def test_to_xarray(self):
        # TODO: Need smarter test in case OOI Server is down. Need caching of the sample netCDF!
        self.OOI._data_urls = self._data_urls

        dataset_list = self.ooi.to_xarray()
        assert isinstance(dataset_list, list)
        assert all(isinstance(data, xr.Dataset) for data in dataset_list)
