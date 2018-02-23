from __future__ import (absolute_import,
                        division,
                        print_function,
                        unicode_literals)

import os
import pytest
import datetime

import xarray as xr

from visualocean.core import OOIASSET


class TestOOIASSET(object):

    def setup(self):
        self.reference_designator = 'RS01SBPS-PC01A-4A-CTDPFA103'

        self.site = 'RS01SBPS'
        self.node = 'PC01A'
        self.sensor = '4A-CTDPFA103'
        self.method = 'streamed'
        self.stream = 'ctdpf_optode_sample'
        self.thredds_url = 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180223T010821-RS01SBPS-PC01A-4A-CTDPFA103-streamed-ctdpf_optode_sample/catalog.html'  # noqa

        self.asset = None

        self.start = datetime.datetime(2017, 8, 21)
        self.end = datetime.datetime(2017, 8, 22)

    def test_OOIASSET(self):
        assettest = OOIASSET(site=self.site,
                             node=self.node,
                             sensor=self.sensor,
                             method=self.method,
                             stream=self.stream)

        assert isinstance(assettest, OOIASSET)

    def test_from_reference_designator(self):
        self.asset = OOIASSET.from_reference_designator(self.reference_designator)

        assert isinstance(self.asset, OOIASSET)

    def test_request_data(self):
        # Due to the sensitivity of credentials,
        # this is tested manually
        pass

    def test_to_xarray(self):
        asset = OOIASSET.from_reference_designator(self.reference_designator)
        asset.thredds_url = self.thredds_url
        ds = asset.to_xarray()

        assert isinstance(ds, xr.Dataset)