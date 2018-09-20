from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime

import os
import pandas as pd
import pytest

import xarray as xr

from yodapy.datasources import OOI
from yodapy.utils.creds import set_credentials_file
from yodapy.utils.parser import get_midnight


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
        self.dt_val = datetime.datetime.utcnow()
        set_credentials_file(data_source='ooi', username=os.environ.get('OOI_USERNAME'), token=os.environ.get('OOI_TOKEN'))
        self.search_results = self.OOI.search(region=self.region,
                                         site=self.site,
                                         instrument=self.instrument)
        
    def test_search(self):

        assert isinstance(self.search_results, OOI)
        assert len(self.search_results) == 2

    def test_view_instruments(self):
        inst = self.OOI.view_instruments()

        assert isinstance(inst, pd.DataFrame)

    def test_view_regions(self):
        inst = self.OOI.view_regions()

        assert isinstance(inst, pd.DataFrame)

    def test_view_sites(self):
        inst = self.OOI.view_sites()

        assert isinstance(inst, pd.DataFrame)

    def test_data_availibility(self):

        assert isinstance(self.search_results.data_availability(), dict)
        assert isinstance(self.search_results._get_cloud_thredds_url(self.search_results._filtered_instruments.iloc[0]), str)

    def test_to_xarray(self):
        # TODO: Need smarter test in case OOI Server is down. Need caching of the sample netCDF!
        self.OOI._data_urls = self._data_urls
        dataset_list = self.OOI.to_xarray()

        assert isinstance(dataset_list, list)
        assert all(isinstance(data, xr.Dataset) for data in dataset_list)

    def test_midnight_check(self):
        midnight = get_midnight(self.dt_val)
        
        assert isinstance(midnight, datetime.datetime)  

    def test_request_data(self):
        data_request = self.search_results.request_data(begin_date=self.start_date,
                                         end_date=self.end_date,
                                         data_type='netcdf')

        assert isinstance(data_request._data_urls, list)
        assert data_request._data_urls
        assert data_request._data_type == 'netcdf'
    
    def test_request_data_check(self):
        self.search_results._data_urls = self._data_urls
        url_len = self.search_results._perform_check()

        assert isinstance(url_len, list)
        assert url_len


        
