# -*- coding: utf-8 -*-

from __future__ import (
    absolute_import,
    division,
    print_function,
    unicode_literals,
)

import datetime
import gc
import logging
import os
import re

import echopype
import gevent
import pytz
import requests
import xarray as xr

from dateutil import parser
from echopype.convert import ConvertEK60
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from yodapy.utils.meta import create_folder
from yodapy.utils.parser import get_nc_urls


if echopype.__version__ == "0.1.21":
    from echopype.model import EchoData
else:
    from echopype.model import EchoDataEK60 as EchoData

logger = logging.getLogger(__name__)


def requests_retry_session(
    retries=10,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504, 404),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_url(
    prepped_request, session=None, timeout=120, stream=False, **kwargs
):

    session = session or requests.Session()
    r = session.send(prepped_request, timeout=timeout, stream=stream, **kwargs)

    if r.status_code == 200:
        logger.debug(f"URL fetch {prepped_request.url} successful.")
        return r
    elif r.status_code == 500:
        message = "Server is currently down."
        if "ooinet.oceanobservatories.org/api" in prepped_request.url:
            message = "UFrame M2M is currently down."
        logger.error(message)
        return r
    else:
        message = f"Request {prepped_request.url} failed: {r.status_code}, {r.reason}"
        logger.error(message)  # noqa
        return r


# --- OOI Data Source Specific connection methods ---
def get_download_urls(url):
    r = requests.get(url)
    tree = html.fromstring(r.content)
    # Only get netCDF
    nc_list = list(
        filter(
            lambda x: re.match(r"(.*?.nc$)", x.attrib["href"]) is not None,
            tree.xpath('//*[contains(@href, ".nc")]'),
        )
    )
    return ["/".join([url, nc.attrib["href"]]) for nc in nc_list]


def download_url(url, data_fold, session):
    """ Perform download and check netcdf download url """
    ds = None
    while ds is None:
        try:
            fname = download_nc(url, folder=data_fold)
            if fname:
                logger.info(f"--- Checking {fname} ---")
                ds = xr.open_dataset(os.path.join(data_fold, fname))
                if isinstance(ds, xr.Dataset):
                    logger.info(f"--- Checks passed for {fname} ---")
        except Exception:
            pass
    del ds
    gc.collect()
    return fname


def download_nc(url, session=None, folder=os.path.curdir):
    """ Prepare request for download and write to netcdf once downloaded """
    session = session or requests.Session()
    name = os.path.basename(url)

    logger.info(f"Downloading {name}...")
    r = requests.Request("GET", url)
    prepped = r.prepare()
    rncdownload = fetch_url(prepped, session=session, stream=True)

    write_nc(name, rncdownload, folder)

    return name


def write_nc(fname, r, folder):
    """ Write to netcdf whatever netcdf url we got """
    with open(os.path.join(folder, fname), "wb") as f:
        logger.info(f"Writing {fname}...")
        # TODO: Add download progressbar?
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    logger.info(f"{fname} successfully downloaded ---")


def fetch_xr(params, **kwargs):
    turl, ref_degs = params
    if kwargs.get("cloud_source"):
        filt_ds = get_nc_urls(
            turl,
            cloud_source=True,
            begin_date=kwargs.get("begin_date"),
            end_date=kwargs.get("end_date"),
        )
        # cleanup kwargs
        kwargs.pop("begin_date")
        kwargs.pop("end_date")
        kwargs.pop("cloud_source")
    else:
        datasets = get_nc_urls(turl)
        # only include instruments where ref_deg appears twice (i.e. was in original filter)
        filt_ds = list(
            filter(
                lambda x: any(x.count(ref) > 1 for ref in ref_degs), datasets
            )
        )

    # TODO: Place some chunking here
    return xr.open_mfdataset(filt_ds, engine="netcdf4", **kwargs)


def instrument_to_query(
    ooi_url="",
    site_rd="",
    infrastructure_rd="",
    instrument_rd="",
    stream_rd="",
    stream_method="",
    stream_start=None,
    stream_end=None,
    begin_ts=None,
    end_ts=None,
    time_check=True,
    exec_dpa=True,
    application_type="netcdf",
    provenance=False,
    limit=-1,
    email=None,
    **kwargs,
):
    """ Get instrument attributes and begin and end dates,
    and convert them to request url from M2M """

    data_url = "/".join(
        [
            ooi_url,
            site_rd,
            infrastructure_rd,
            instrument_rd,
            stream_method,
            stream_rd,
        ]
    )

    # Check for application_type
    application_type = application_type.lower()
    if application_type not in ["netcdf", "json"]:
        text = f"Invalid application type/format: {application_type}"
        logger.error(text)
        raise ValueError(text)

    if application_type == "json":
        if limit <= 0:
            text = "Please specify data points limit!"
            logger.error(text)
            raise ValueError(text)

    # Prepare time request
    begin_dt = None
    end_dt = None
    if begin_ts:
        try:
            begin_dt = parser.parse(begin_ts).replace(tzinfo=pytz.UTC)
        except ValueError as e:
            logger.error(f"Invalid begin_dt: {begin_ts} ({e})")
            return None

    if end_ts:
        try:
            end_dt = parser.parse(end_ts).replace(tzinfo=pytz.UTC)
        except ValueError as e:
            logger.error(f"Invalid end_dt: {end_ts} ({e})")
            return None

    # Check time
    if time_check:
        stream_dt1 = parser.parse(stream_end).replace(tzinfo=pytz.UTC)
        stream_dt0 = parser.parse(stream_start).replace(tzinfo=pytz.UTC)
        if end_dt > stream_dt1:
            logger.warning(
                f'{"-".join([site_rd, infrastructure_rd, instrument_rd, stream_method, stream_rd])} time check - End time exceeds stream endTime'
            )  # noqa
            logger.warning("Setting request end time to stream endTime")
            end_dt = stream_dt1

        if begin_dt < stream_dt0:
            logger.warning(
                f'{"-".join([site_rd, infrastructure_rd, instrument_rd, stream_method, stream_rd])} time check - Start time is earlier than stream beginTime'
            )  # noqa
            logger.warning("Setting request begin time to stream beginTime")
            begin_dt = stream_dt0

        if begin_dt >= end_dt:
            logger.warning(
                f"Invalid time range specified: {begin_dt.isoformat()} to {end_dt.isoformat()}"
            )  # noqa
            raise ValueError(
                f"Invalid time range specified: {begin_dt.isoformat()} to {end_dt.isoformat()}"
            )  # noqa

    payload = {
        "beginDT": begin_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "endDT": end_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "format": f"application/{application_type}",
        "limit": limit,
        "execDPA": str(exec_dpa).lower(),
        "include_provenance": str(provenance).lower(),
    }
    if email:
        payload["email"] = email

    return (data_url, payload)


def download_raw_file(source_folder, ref, raw):
    """
    Args:
        ref (str): String. Reference to site and platform.
        raw (Series): Pandas Series. Information containing raw url and filename
    """

    temp_folder = os.path.join(source_folder, ref)
    if not os.path.exists(temp_folder):
        os.mkdir(temp_folder)

    temp_file = os.path.join(temp_folder, raw["filename"])
    print(
        f"{datetime.datetime.now().strftime('%H:%M:%S')}  downloading: {os.path.basename(temp_file)}"
    )
    if os.path.exists(temp_file):
        print("          ... this file already exists, skipping download.")
        return temp_file
    else:
        r = requests.get(raw["urls"], allow_redirects=True)
        if r.status_code == 200:
            with open(temp_file, mode="wb") as rawfile:
                rawfile.write(r.content)
            return temp_file
        else:
            print(r.status_code)
            return None


def get_processed_ek60(temp_file, clean_up=True):
    calibrated = temp_file.replace(".raw", "_Sv.nc")
    calibrated_cleaned = temp_file.replace(".raw", "_Sv_clean.nc")
    mvbs = temp_file.replace(".raw", "_MVBS.nc")

    data_tmp = ConvertEK60(temp_file)
    data_tmp.raw2nc()

    data = EchoData(temp_file.replace(".raw", ".nc"))

    # Calibration and echo-integration
    if os.path.exists(calibrated):
        os.unlink(calibrated)
    data.calibrate(save=True)

    # Denoising
    if os.path.exists(calibrated_cleaned):
        os.unlink(calibrated_cleaned)
    data.remove_noise(save=True)

    # Mean Volume Backscatter Strength
    if os.path.exists(mvbs):
        os.unlink(mvbs)
    data.get_MVBS(save=True)

    if os.path.exists(mvbs):
        if clean_up:
            print(
                f"{datetime.datetime.now().strftime('%H:%M:%S')}  cleaning up: {temp_file.replace('.raw', '.nc')}"
            )
            os.unlink(temp_file.replace(".raw", ".nc"))
            print(
                f"{datetime.datetime.now().strftime('%H:%M:%S')}  cleaning up: {temp_file.replace('.raw', '_Sv.nc')}"
            )
            os.unlink(temp_file.replace(".raw", "_Sv.nc"))
            print(
                f"{datetime.datetime.now().strftime('%H:%M:%S')}  cleaning up: {temp_file.replace('.raw', '_Sv_clean.nc')}"
            )
            os.unlink(temp_file.replace(".raw", "_Sv_clean.nc"))
        return mvbs


def perform_ek60_download(filtered_datadf, source_name="ooi", timeout=3600):
    raw_files = {}
    for ref, raw_df in filtered_datadf.items():
        raw_files[ref] = []
        source_folder = create_folder(source_name)
        if os.path.exists(source_folder):
            jobs = [
                gevent.spawn(download_raw_file, source_folder, ref, raw)
                for idx, raw in raw_df.iterrows()
            ]
            gevent.joinall(jobs, timeout=timeout)
            for job in jobs:
                raw_files[ref].append(job.value)
    return raw_files


def perform_ek60_processing(raw_file_dict, timeout=3600, clean_up=True):
    mvbs_files = {}
    for ref, raw_files in raw_file_dict.items():
        mvbs_files[ref] = []
        jobs = [
            gevent.spawn(get_processed_ek60, raw, clean_up)
            for raw in raw_files
        ]
        gevent.joinall(jobs, timeout=timeout)
        for job in jobs:
            mvbs_files[ref].append(job.value)
    return mvbs_files


# --- End OOI Data Source Specific connection methods ---
