from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime

import os
import pandas as pd
import pytest
import unittest.mock as mock

import xarray as xr

from yodapy.datasources import OOI
from yodapy.datasources.ooi import helpers
from yodapy.datasources.ooi.m2m_client import M2MClient
from yodapy.utils.creds import set_credentials_file
from yodapy.utils.parser import get_midnight, get_nc_urls
from unittest.mock import patch


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
        self.user = 'Test'
        self.stream = 'ctdpf_optode_calibration_coefficients'
        self.ref_designator = 'RS03AXPS-PC03A-4A-CTDPFA303'
        
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

    @patch('builtins.input', side_effect= ['yes'])
    def test_to_xarray(self, input):
        data_request = self.search_results.request_data(begin_date=self.start_date,
                                         end_date=self.end_date,
                                         data_type='netcdf')
        dataset_list = data_request.to_xarray()

        download_nc_dataset_list = data_request.download_ncfiles()

        assert isinstance(download_nc_dataset_list, list)
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
        turls = self.search_results._perform_check()
        nc_urls = get_nc_urls(turls[0], download=True)
        ncurl_list = helpers.filter_ncurls(nc_urls, begin_date = self.start_date, end_date = self.end_date)

        assert isinstance(ncurl_list, list)
        assert isinstance(turls, list)
        assert ncurl_list
        assert turls

    def test_preferred_stream_availability(self):
        inst = pd.DataFrame({'reference_designator': 'RS03AXPS-PC03A-4A-CTDPFA303',
                         'name': 'CTD',
                         'preferred_stream': ''
                        }, index=[0])
        inst_stream_incorrect = pd.DataFrame({'reference_designator': 'RS03AXPS-PC03A-4A-CTDPFA303',
                         'name': 'CTD',
                         'preferred_stream': 'ctdpf'
                        }, index=[0])
        
        inst_availability = self.OOI._retrieve_availibility(inst)
        inst_stream_missing_availability = self.OOI._retrieve_availibility(inst, stream_type='eScience')
        inst_stream_incorrect_availability = self.OOI._retrieve_availibility(inst_stream_incorrect)


        assert isinstance(inst_availability, dict)
        assert not inst_availability
        assert isinstance(inst_stream_missing_availability, dict)
        assert not inst_stream_missing_availability
        assert isinstance(inst_stream_incorrect_availability, dict)
        assert not inst_stream_incorrect_availability

    def test_reference_designator_false_availability(self):
        inst = pd.DataFrame({'reference_designator': 'RS03AXPS-PC03A-4A',
                         'name': 'CTD',
                         'preferred_stream': ''
                        }, index=[0])
        with pytest.raises(TypeError):
            self.OOI._retrieve_availibility(inst)

    def test_m2m_client_response_check(self):
        m2m_client = M2MClient()

        response_parameters = m2m_client.fetch_instrument_parameters(ref_des = 'RS03AXPS-PC03A-4A-CTDPFA303')
        response_metadata = m2m_client.fetch_instrument_metadata(ref_des = 'RS03AXPS-PC03A-4A-CTDPFA303')
        response_deployment = m2m_client.fetch_instrument_deployments(ref_des = 'RS03AXPS-PC03A-4A-CTDPFA303')

        assert isinstance(response_parameters, list)
        assert isinstance(response_metadata, dict)
        assert isinstance(response_deployment, list)
        assert response_parameters
        assert response_metadata
        assert response_deployment
    
    def test_m2m_client_urls(self):
        m2m_client = M2MClient()

        request_urls = m2m_client.instrument_to_query(ref_des = 'RS03AXPS-PC03A-4A-CTDPFA303', user=self.user, limit=10, time_delta_type='months',
                                                 time_delta_value= 1, begin_ts=self.start_date, end_ts=self.end_date, stream=self.stream)
        request_urls_empty = m2m_client.instrument_to_query(ref_des = 'RS03AXPS-PC03A-4A-CTDPFA303', user=self.user, limit=10, time_delta_type='months',
                                                 time_delta_value= 1, begin_ts='2018-13-01', end_ts=self.end_date, stream=self.stream)
        assert isinstance(request_urls, list)
        assert request_urls
        assert isinstance(request_urls_empty, list)
        assert not request_urls_empty

