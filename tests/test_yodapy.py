from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import datetime
import pytest

import xarray as xr

from yodapy.datasource import OOI

class testOOI():

    def __init__(self):
        self.region = 'Cabled'
        self.site = 'Axial Base Shallow Profiler Mooring'
        self.platform = 'Shallow Profiler'
        self.instrument = 'CTD'
        self.start_date = datetime.datetime(2018, 4, 1)
        self.end_date = datetime.datetime(2018, 4, 2)
        self.thredds_url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/pshivraj@uw.edu/20180523T175502-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'  # noqa
        self._status_url = 'https://opendap.oceanobservatories.org/async_results/pshivraj@uw.edu/20180523T175502-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample'  # noqa


    def test_search_type(self):
        assert isinstance(self.region, (list, str))
        assert isinstance(self.site, (list, str))
        assert isinstance(self.instrument, (list, str))
        assert isinstance(self.start_date, datetime.datetime)
        assert isinstance(self.end_date, datetime.datetime)

    def test_search_object(self):
        search_results = OOI.search(region=self.region,
                                    site=self.site,
                                    platform=self.platform,
                                    instrument=self.instrument,
                                    start_date=self.start_date,
                                    end_date=self.end_date)

        assert isinstance(search_results, list)
        assert all(isinstance(data, object) for data in search_results)


    def test_request_to_xarray(self):
        thredds_url = self.thredds_url
        _status_url = self._status_url
        request_data = OOI.Requests.to_xarray(thredds_url, _status_url, **kwargs)

        assert isinstance(request_data, list)
        assert all(isinstance(data, xr.Dataset) for data in request_data)
