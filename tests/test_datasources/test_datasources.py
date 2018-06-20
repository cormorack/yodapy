# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import datetime

from yodapy.datasources.datasource import DataSource


class TestDataSource:

    def setup(self):
        self.ds = DataSource()
        self.ds._source_name = 'ooi'
        self.ds._start_date = datetime.datetime(2013, 1, 1)
        self.ds._end_date = datetime.datetime(2018, 7, 7)

    def test_init(self):
        assert isinstance(self.ds, DataSource)

    def test_source_name(self):
        assert self.ds.source_name == 'ooi'

    def test_start_date(self):
        assert self.ds.start_date == '{:%Y-%m-%d}'.format(datetime.datetime(2013, 1, 1))  # noqa

    def test_end_date(self):
        assert self.ds.end_date == '{:%Y-%m-%d}'.format(datetime.datetime(2018, 7, 7))  # noqa
