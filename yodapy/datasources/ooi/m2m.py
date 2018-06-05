# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
from urllib.parse import urljoin

import requests


class MachineToMachine:
    def __init__(self, api_user, api_key):
        self.base_url = 'https://ooinet.oceanobservatories.org/api/m2m/'
        self.api_user = api_user
        self.api_key = api_key
        self.preload_url = urljoin(self.base_url, '12575')
        self.inv_url = urljoin(self.base_url, os.path.join('12576', 'sensor', 'inv'))
        self.meta_url = urljoin(self.base_url, os.path.join('12587', 'events', 'deployment', 'inv'))

    def requests(self, url):
        return requests.get(url, auth=(self.api_user, self.api_key)).json()

    def toc(self):
        return self.requests(os.path.join(self.inv_url, 'toc'))

    def get_param_info(self, paramid):
        endpoint = 'parameter'
        url = os.path.join(self.preload_url, endpoint, str(paramid))
        return self.requests(url)

    def get_stream_info(self, stream_rd):
        endpoint = os.path.join('stream', 'byname')
        url = os.path.join(self.preload_url, endpoint, str(stream_rd))
        return self.requests(url)

    def instrument_stream_times(self, subsite, node, sensor):
        url = os.path.join(self.inv_url, subsite, node, sensor, 'metadata',
                           'times')
        return self.requests(url)

    def subsite_inventory(self, subsite):
        url = os.path.join(self.inv_url, subsite)
        return self.requests(url)

    def node_inventory(self, subsite, node):
        url = os.path.join(self.inv_url, subsite, node)
        return ['-'.join((subsite, node, sensor)) for sensor in self.requests(url)]

    def metadata(self, subsite, node, sensor):
        url = os.path.join(self.meta_url, subsite, node, sensor, '-1')
        return self.requests(url)

    def streams(self):
        toc = self.toc()
        stream_map = {}
        toc = toc['instruments']
        for row in toc:
            rd = row['reference_designator']
            for each in row['streams']:
                stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])
        return stream_map

    def instruments(self):
        """return list of all instruments in the system"""
        nodes = []
        for subsite in self.requests(self.inv_url):
            for node in self.requests(os.path.join(self.inv_url, subsite)):
                nodes.extend(self.node_inventory(subsite, node))
        return nodes