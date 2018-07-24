# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import datetime
import re
from urllib.parse import urljoin, urlsplit

from lxml import etree
import netCDF4 as nc
import requests


def get_nc_urls(thredds_url):
    caturl = thredds_url.replace('.html', '.xml')

    parsed_uri = urlsplit(caturl)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)

    response = requests.get(caturl)
    ROOT = etree.XML(response.content)
    dataset_el = list(filter(lambda x: re.match(r'(.*?.nc$)',
                             x.attrib['urlPath']) is not None,
                             ROOT.xpath('//*[contains(@urlPath, ".nc")]')))

    service_el = ROOT.xpath('//*[contains(@name, "odap")]')[0]

    dataset_urls = [urljoin(domain,
                            urljoin(service_el.attrib['base'],
                                    el.attrib['urlPath'])) for el in dataset_el]  # noqa

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
