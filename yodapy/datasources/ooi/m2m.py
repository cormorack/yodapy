# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
from urllib.parse import urljoin

import requests
from requests import (Session, Request)


from yodapy.utils.parser import ooi_instrument_reference_designator
from yodapy.utils.conn import requests_retry_session


class MachineToMachine:
    def __init__(self, api_user, api_key):
        self.base_url = 'https://ooinet.oceanobservatories.org/api/m2m/'
        self.api_user = api_user
        self.api_key = api_key
        self.preload_url = urljoin(self.base_url, '12575')
        self.inv_url = urljoin(self.base_url,
                               os.path.join('12576', 'sensor', 'inv'))
        self.meta_url = urljoin(self.base_url,
                                os.path.join('12587', 'events',
                                             'deployment', 'inv'))

    @classmethod
    def use_existing_credentials(cls):
        home_dir = os.environ.get('HOME')
        fpath = os.path.join(home_dir, '.netrc')

        if os.path.exists(fpath):
            import netrc
            netrc = netrc.netrc()
            remoteHostName = 'ooinet.oceanobservatories.org'
            info = netrc.authenticators(remoteHostName)
            return cls(info[0], info[2])
        else:
            raise EnvironmentError('Please authenticate by using '
                                   'yodapy.utils.set_ooi_credentials_file!')

    def requests(self, url):
        return requests.get(url, auth=(self.api_user, self.api_key)).json()

    def _availibility_check(self, stream, params):
        availdict = self._stream_availibility(**self.desired_data(stream))
        begin = params['beginDT']
        end = params['endDT']

        if begin < availdict['beginTime']:
            raise Warning('No data is available before {}'.format(availdict[
                                                                 'beginTime']))  # noqa
        if end > availdict['endTime']:
            raise Warning('No data is available after {}'.format(availdict[
                                                                 'endTime']))  # noqa

    def data_requests(self, session, stream, params, **kwargs):
        data_url = self.create_data_url(**self.desired_data(stream))

        try:
            self._availibility_check(stream, params)

            data = None
            while not data:
                get_req = requests_retry_session(session=session, **kwargs).get(
                    data_url,
                    auth=(self.api_user, self.api_key),
                    params=params)
                data = get_req.json()

            return data
        except Exception as e:
            print(e)

    def create_data_url(self, subsite, node, sensor, method, stream):
        return os.path.join(self.inv_url, subsite,
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
        return ['-'.join((subsite, node, sensor)) for sensor in self.requests(url)]  # noqa

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
                stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])  # noqa
        return stream_map

    def instruments(self):
        """return list of all instruments in the system"""
        nodes = []
        for subsite in self.requests(self.inv_url):
            for node in self.requests(os.path.join(self.inv_url, subsite)):
                nodes.extend(self.node_inventory(subsite, node))
        return nodes