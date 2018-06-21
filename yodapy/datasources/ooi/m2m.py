# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os

import requests

from yodapy.utils.files import CREDENTIALS_FILE
from yodapy.utils.parser import (build_url,
                                 ooi_instrument_reference_designator)
from yodapy.utils.conn import requests_retry_session


class MachineToMachine:
    def __init__(self, api_user, api_key):
        self.base_url = 'https://ooinet.oceanobservatories.org/api/m2m'
        self.api_user = api_user
        self.api_key = api_key
        self.preload_url = build_url(self.base_url, '12575')
        self.inv_url = build_url(self.base_url, '12576', 'sensor', 'inv')
        self.meta_url = build_url(self.base_url, '12587',
                                  'events', 'deployment', 'inv')

    @classmethod
    def use_existing_credentials(cls):

        if os.path.exists(CREDENTIALS_FILE):
            import json
            with open(CREDENTIALS_FILE) as f:
                creds = json.load(f)['ooi']
                return cls(creds['username'], creds['api_key'])
        else:
            raise EnvironmentError('Please authenticate by using '
                                   'yodapy.utils.set_ooi_credentials_file!')

    def requests(self, url):
        req = requests.get(url, auth=(self.api_user, self.api_key))
        if req.status_code == 200:
            return req.json()
        else:
            raise Exception('{}'.format(req.text))

    def _availibility_check(self, stream, params):
        availdict = self._stream_availibility(**self.desired_data(stream))
        begin = params['beginDT']
        if begin < availdict['beginTime']:
            raise Warning('No data is available before {}'.format(availdict[
                                                                 'beginTime']))  # noqa

        if 'endDT' in params:
            end = params['endDT']
            if end > availdict['endTime']:
                raise Warning('No data is available after {}'.format(availdict[
                                                                     'endTime']))  # noqa

    def data_requests(self, session, stream, params, **kwargs):
        data_url = self.create_data_url(**self.desired_data(stream))

        try:
            self._availibility_check(stream, params)

            data = None
            while not data:
                get_req = requests_retry_session(session=session,
                                                 **kwargs).get(data_url,
                                                               auth=(self.api_user, self.api_key),  # noqa
                                                               params=params)
                data = get_req.json()

            return data
        except Exception as e:
            print(e)

    def create_data_url(self, subsite, node, sensor, method, stream):
        return build_url(self.inv_url, subsite,
                         node, sensor, method,
                         stream)

    def _stream_availibility(self, subsite, node,
                             sensor, stream, **kwargs):
        return next(filter(lambda x: x['stream'] == stream,
                           self.instrument_stream_times(subsite,
                                                        node,
                                                        sensor)))

    @staticmethod
    def desired_data(stream):
        dd = {
            'stream': stream['stream'],
            'method': stream['stream_method']
        }
        dd.update(
            ooi_instrument_reference_designator(stream[
                                                    'reference_designator']))

        return dd

    def toc(self):
        return self.requests(build_url(self.inv_url, 'toc'))

    def get_param_info(self, paramid):
        endpoint = 'parameter'
        url = build_url(self.preload_url, endpoint, str(paramid))
        return self.requests(url)

    def get_stream_info(self, stream_rd):
        endpoint = build_url('stream', 'byname')
        url = build_url(self.preload_url, endpoint, str(stream_rd))
        return self.requests(url)

    def instrument_stream_times(self, subsite, node, sensor):
        url = build_url(self.inv_url, subsite, node, sensor, 'metadata',
                           'times')
        return self.requests(url)

    def subsite_inventory(self, subsite):
        url = build_url(self.inv_url, subsite)
        return self.requests(url)

    def node_inventory(self, subsite, node):
        url = build_url(self.inv_url, subsite, node)
        return ['-'.join((subsite, node, sensor)) for sensor in self.requests(url)]  # noqa

    def metadata(self, subsite, node, sensor):
        url = build_url(self.meta_url, subsite, node, sensor, '-1')
        return self.requests(url)

    def streams(self):
        toc = self.toc()
        stream_map = {}
        toc = toc['instruments']
        for row in toc:
            rd = row['reference_designator']
            for each in row['streams']:
                stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])  # noqa
        return stream_map

    def instruments(self):
        """return list of all instruments in the system"""
        nodes = []
        for subsite in self.requests(self.inv_url):
            for node in self.requests(build_url(self.inv_url, subsite)):
                nodes.extend(self.node_inventory(subsite, node))
        return nodes