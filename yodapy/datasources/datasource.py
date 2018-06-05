# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)


class DataSource:

    def __init__(self):
        self._source_name = None
        self._start_time = None
        self._end_time = None

    def __repr__(self):
        return 'Data Source: {0}'.format(self._source_name)

    def __len__(self):
        pass

    @property
    def start_date(self):
        if self._start_time:
            return '{:%Y-%m-%d}'.format(self._start_time)
        return 'Start date can\'t be found.'

    @property
    def end_date(self):
        if self._end_time:
            return '{:%Y-%m-%d}'.format(self._end_time)
        return 'End date can\'t be found.'

    @property
    def source_name(self):
        return self._source_name

    def request_data(self):
        pass

    def to_xarray(self):
        pass
