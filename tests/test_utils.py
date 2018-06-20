# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import datetime
import os

from requests import Session

from yodapy.utils import (conn,
                          creds,
                          meta,
                          parser)
from yodapy.utils.meta import (HOME_DIR,
                               YODAPY_PATH)


def test_request_retry_session():
    session = conn.requests_retry_session()
    req = session.get('https://google.com')

    assert isinstance(session, Session)
    assert req.status_code == 200


def test_set_ooi_credentials_file():
    username = 'testuser'
    token = 'te$tT0k3n'
    creds.set_ooi_credentials_file(username=username,
                                   token=token)

    fpath = os.path.join(HOME_DIR, '.netrc')

    assert os.path.exists(fpath)

    import netrc
    netrc = netrc.netrc()
    remote_host_name = 'ooinet.oceanobservatories.org'
    info = netrc.authenticators(remote_host_name)

    assert info[0] == username
    assert info[2] == token


def test_create_folder():
    source_name = 'OOI'
    res_path = meta.create_folder(source_name=source_name)

    fold_name = source_name.lower()
    fold_path = os.path.join(YODAPY_PATH, fold_name)

    assert os.path.exists(YODAPY_PATH)
    assert os.path.exists(fold_path)
    assert res_path == fold_path


def test_get_nc_urls():
    thredds_url = 'https://opendap.oceanobservatories.org/thredds' \
                  '/catalog/ooi/landungs@uw.edu/20180606T232135' \
                  '-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_' \
                  'optode_sample/catalog.html'

    dataset_urls = parser.get_nc_urls(thredds_url=thredds_url)
    result_test = ['https://opendap.oceanobservatories.org/thredds'
                   '/dodsC/ooi/landungs@uw.edu/20180606T232135'
                   '-RS03AXPS-PC03A-4A-CTDPFA303-streamed-ctdpf_'
                   'optode_sample/deployment0004_RS03AXPS-PC03A-4A'
                   '-CTDPFA303-streamed-ctdpf_optode_sample_20180101T000000.'
                   '596438-20180131T235959.815406.nc']

    assert isinstance(dataset_urls, list)
    assert dataset_urls == result_test


def test_unix_time_millis():
    dt = datetime.datetime(2018, 7, 7)
    mill = parser.unix_time_millis(dt=dt)

    assert isinstance(mill, int)
    assert mill == 1530921600000


def test_datetime_to_string():
    dt = datetime.datetime(2018, 7, 7)
    dtstring = parser.datetime_to_string(dt=dt)

    assert isinstance(dtstring, str)
    assert dtstring == '2018-07-07T00:00:00.000000Z'


def test_seconds_to_date():
    seconds = 3713508070.3271585
    sec_result = parser.seconds_to_date(seconds)

    assert isinstance(sec_result, datetime.datetime)
    assert sec_result == datetime.datetime(2017, 9, 4, 10, 1, 10, 327158)


def test_ooi_instrument_reference_designator():
    rd = 'RS03ASHS-MJ03B-07-TMPSFA301'
    rddict = parser.ooi_instrument_reference_designator(reference_designator=rd)  # noqa

    assert isinstance(rddict, dict)
    assert rddict == {'subsite': 'RS03ASHS',
                      'node': 'MJ03B',
                      'sensor': '07-TMPSFA301'}


def test_build_url():
    base_url = 'https://ooinet.oceanobservatories.org/api/m2m'
    preload_url = parser.build_url(base_url, '12575')
    inv_url = parser.build_url(base_url, '12576', 'sensor', 'inv')
    meta_url = parser.build_url(base_url, '12587', 'events',
                                'deployment', 'inv')

    assert preload_url == f'{base_url}/12575'
    assert inv_url == f'{base_url}/12576/sensor/inv'
    assert meta_url == f'{base_url}/12587/events/deployment/inv'
