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

from yodapy.utils.conn import requests_retry_session
from yodapy.utils.parser import get_nc_urls

logger = logging.getLogger(__name__)


def check_data_status(session, data, **kwargs):
    urls = {
        'thredds_url': data['allURLs'][0],
        'status_url': data['allURLs'][1]
    }
    check_complete = os.path.join(urls['status_url'], 'status.txt')

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
    datasets = get_nc_urls(turl, begin_date=kwargs.get('begin_date'), end_date=kwargs.get('end_date'))
    if 'begin_date' in kwargs.keys():
        kwargs.pop('begin_date')
    if 'end_date' in kwargs.keys():
        kwargs.pop('end_date')
    # only include instruments where ref_deg appears twice (i.e. was in original filter)
    filt_ds = list(filter(lambda x: any(x.count(ref) > 1 for ref in ref_degs), datasets))
    return xr.open_mfdataset(
        filt_ds,
        preprocess=preprocess_ds,
        decode_times=False,
        engine='netcdf4',
        **kwargs)
