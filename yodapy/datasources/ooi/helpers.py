# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import logging
import os

import dask
import xarray as xr
import requests
import gevent
import pandas as pd

from yodapy.utils.conn import requests_retry_session
from yodapy.utils.parser import get_nc_urls

logger = logging.getLogger(__name__)


def check_data_status(session, data, **kwargs):
    urls = {
        'thredds_url': data['allURLs'][0],
        'status_url': data['allURLs'][1]
    }
    check_complete = '/'.join([urls['status_url'], 'status.txt'])

    req = None
    print('\nYour data ({}) is still compiling... Please wait.'.format(
        os.path.basename(urls['status_url'])))
    while not req:
        req = requests_retry_session(session=session, **kwargs).get(
            check_complete)
    print('\nRequest completed.')  # noqa

    return urls['thredds_url']


def preprocess_ds(ds):
    cleaned_ds = ds.swap_dims({'obs': 'time'})
    logger.debug('DIMS SWAPPED')
    return cleaned_ds


def write_nc(fname, r, folder):
    with open(os.path.join(folder, fname), 'wb') as f:
        logger.info(f'Writing {fname}...')
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)


def download_nc(url, folder=os.path.curdir):
    name = os.path.basename(url)

    logger.info(f'Downloading {name}...')
    r = requests.get(url, stream=True)
    write_nc(name, r, folder)

    return name


def download_all_nc(turl, folder):
    nc_urls = get_nc_urls(turl, download=True)

    jobs = [gevent.spawn(download_nc, url, folder) for url in nc_urls]
    gevent.joinall(jobs, timeout=300)
    ncfiles = [job.value for job in jobs]
    return ncfiles


def fetch_xr(params, **kwargs):
    turl, ref_degs = params
    datasets = get_nc_urls(turl)
    if kwargs.get('cloud_source'):
        datasets = filter_ncurls(datasets,
                                 begin_date=kwargs.get('begin_date'),
                                 end_date=kwargs.get('end_date'))

        # cleanup kwargs
        kwargs.pop('begin_date')
        kwargs.pop('end_date')
        kwargs.pop('cloud_source')

    # only include instruments where ref_deg appears twice (i.e. was in original filter)
    filt_ds = list(filter(lambda x: any(x.count(ref) > 1 for ref in ref_degs), datasets))
    # TODO: Place some chunking here
    # TODO: For cloud copy, how do I filter a specific time?
    return xr.open_mfdataset(
        filt_ds,
        engine='netcdf4',
        **kwargs)


def get_start_end_dates(ncurl):
    import re
    import pandas as pd

    regex = r'(?P<start_date>\d{4}[01]\d[0123]\dT[012]\d\d{2}\d+.\d+)-(?P<end_date>\d{4}[01]\d[0123]\dT[012]\d\d{2}\d+.\d+)'  # noqa
    sres = re.search(regex, ncurl)
    return {
        'ncurl': ncurl,
        'start_date': pd.to_datetime(sres.group('start_date')),
        'end_date': pd.to_datetime(sres.group('end_date'))
    }


def create_range_df(row):
    dr = pd.date_range(row.start_date,
                       row.end_date,
                       freq='D').to_period(freq='D').to_timestamp()
    bdf = pd.DataFrame()
    bdf.loc[:, 'time'] = dr
    bdf.loc[:, 'ncurl'] = row.ncurl
    return bdf


def filter_ncurls(nc_urls, begin_date, end_date):
    ncdf = pd.DataFrame.from_records([get_start_end_dates(ncurl) for ncurl in nc_urls])[['ncurl', 'start_date', 'end_date']]  # noqa
    clean_ncdf = pd.concat([create_range_df(row) for idx, row in ncdf.iterrows()]).set_index('time').sort_index()  # noqa
    return list(clean_ncdf.loc[begin_date:end_date]['ncurl'].unique())
