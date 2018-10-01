# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import datetime

import netCDF4 as nc
import pandas as pd
import numpy as np
from dateutil import parser

from siphon.catalog import TDSCatalog


def get_nc_urls(thredds_url, download=False, cloud_source=False, **kwargs):
    urltype = 'OPENDAP'
    if download:
        urltype = 'HTTPServer'
    caturl = thredds_url.replace('.html', '.xml')
    cat = TDSCatalog(caturl)
    datasets = cat.datasets
    dataset_urls = []
    if cloud_source:
        # TODO: Add bd and ed time checking, and warn user if data not available.
        bd = parser.parse(kwargs.get('begin_date'))
        ed = parser.parse(kwargs.get('end_date'))
        dataset_urls = list(map(lambda x: x.access_urls[urltype],
                                datasets.filter_time_range(bd, ed,
                                                           regex=r'(?P<year>\d{4})(?P<month>[01]\d)(?P<day>[0123]\d)')))
    else:
        ncfiles = list(filter(lambda d: not any(w in d.name for w in ['json', 'txt', 'ncml']),  # noqa
                        [cat.datasets[i] for i, d in enumerate(datasets)]))  # noqa
        dataset_urls = [d.access_urls[urltype] for d in ncfiles]
    return dataset_urls


def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000)


def datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


def seconds_to_date(num):
    return nc.num2date(num, 'seconds since 1900-01-01')


def get_midnight(dt):
    nextday = dt + datetime.timedelta(1)
    time_labels = ('hour', 'minute', 'second', 'microsecond')
    time_list = nextday.hour, nextday.minute, nextday.second, nextday.microsecond  # noqa
    zeroed = tuple(map(lambda x: 0 if x > 0 else x, time_list))
    return nextday.replace(**dict(zip(time_labels, zeroed)))


def ooi_instrument_reference_designator(reference_designator):
    """
    Parses reference designator into a dictionary containing subsite, node,
    and sensor.

    Args:
        reference_designator (str): OOI Instrument Reference Designator

    Returns:
        Dictionary of the parsed reference designator
    """

    keys = ['subsite', 'node', 'sensor']
    val = reference_designator.split('-')
    values = val[:-2] + ['-'.join(val[-2:])]
    return dict(zip(keys, values))


def build_url(*args):
    return '/'.join(args)


def split_val_list(df, lst_col):
    return pd.DataFrame({
      col: np.repeat(df[col].values,
                     df[lst_col].str.len())
      for col in df.columns.drop(lst_col)}
    ).assign(**{lst_col: np.concatenate(df[lst_col].values)})[df.columns]


def get_value(rowcol):
    if isinstance(rowcol, dict):
        return rowcol['value']
