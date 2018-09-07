# -*- coding: utf-8 -*-
"""
m2m_client.py

Client module for the M2M Interface, originally developed by John Kerfoot.
https://github.com/kerfoot/uframe-m2m
"""

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import os
import logging
import requests
import re
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta
import datetime
import pytz

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import pandas as pd
import progressbar

from yodapy.utils.files import CREDENTIALS_FILE
from yodapy.utils.parser import (get_value, 
                                 split_val_list)

HTTP_STATUS_OK = 200
HTTP_STATUS_NOT_FOUND = 404

DEPLOYMENT_STATUS_TYPES = ['all',
                           'active',
                           'inactive']

_valid_relativedeltatypes = ('years',
                             'months',
                             'weeks',
                             'days',
                             'hours',
                             'minutes',
                             'seconds')


class M2MClient:

    def __init__(self, timeout=120, api_username=None,
                 api_token=None, **kwargs):
        """Lightweight OOI UFrame client for making GET requests to the UFrame API via
        the machine to machine (m2m) API or directly to UFrame.

        Parameters:
            base_url: UFrame API base url which must begin with https://

        kwargs:
            m2m: If true <Default>, specifies that all requests should be created and sent throught the m2m API
            timeout: request timeout, in seconds
            api_username: API username from the UI user settings
            api_token: API password from the UI user settings
        """

        self._base_url = 'https://ooinet.oceanobservatories.org'
        self._m2m_base_url = None
        self._timeout = timeout
        self._api_username = api_username
        self._api_token = api_token
        self._session = requests.Session()
        self._pool_connections = kwargs.get('pool_connections', 100)
        self._pool_maxsize = kwargs.get('pool_maxsize', 100)
        a = requests.adapters.HTTPAdapter(pool_connections=self._pool_connections,  # noqa
                                          pool_maxsize=self._pool_maxsize)
        self._session.mount('https://', a)
        self._is_m2m = True
        self._instruments = []
        self._subsites = []
        self._instrument_streams = []
        self._streams = []
        self._toc = None
        self._parameter_index = None

        self._logger = logging.getLogger(__name__)

        # properties for last m2m request
        self._request_url = None
        self._response = None
        self._status_code = None
        self._reason = None
        self._response_headers = None

        # Set the base url
        self._logger.info(
            'Creating M2mClient instance ({:s})'.format(self._base_url))
        self.base_url = self._base_url

    @property
    def base_url(self):
        return self._base_url

    @property
    def api_username(self):
        return self._api_username

    @property
    def api_token(self):
        return self._api_token

    @base_url.setter
    def base_url(self, url):
        widgets = [
            progressbar.Percentage(),
            ' ', progressbar.Bar(),
            progressbar.FormatLabel('Time elapsed: %(elapsed)s')
        ]
        bar = progressbar.ProgressBar(widgets=widgets, max_value=5).start()

        self._logger.debug('Setting UFrame credentials.')
        if not self._api_username or not self._api_token:
            self._use_existing_credentials()
        bar += 1

        self._logger.debug('Setting UFrame base url: {:s}'.format(url))

        if not url:
            self._logger.warning('No UFrame base_url specified')
            return
        if not url.startswith('http'):
            self._logger.warning('base_url must start with http')
            return

        self._base_url = url.strip('/')
        self._m2m_base_url = '{:s}/api/m2m'.format(self._base_url)

        self._logger.debug('UFrame base_url: {:s}'.format(self.base_url))
        self._logger.debug(
            'UFrame m2m base_url: {:s}'.format(self.m2m_base_url))
        bar += 1

        # Try to get the sensor invetory subsite list to see if we're able to connect
        self.fetch_subsites()
        if self._status_code != HTTP_STATUS_OK:
            self._logger.critical('Unable to connect to UFrame instance')
            self._base_url = None
            # self._valid_uframe = False
            return
        bar += 1

        # Create the instrument list
        self._create_instrument_list()
        bar += 1
        self._create_parameter_index()
        bar += 1
        bar.finish()

    @property
    def is_m2m(self):
        return self._is_m2m

    @is_m2m.setter
    def is_m2m(self, status):
        """Configures the instance to send requests either via the m2m <Default>
        API or directly to the UFrame API"""
        if type(status) != bool:
            self._logger.error('status must be True or False')
            return

        self._is_m2m = status

    @property
    def m2m_base_url(self):
        return self._m2m_base_url

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, seconds):
        if type(seconds) != int:
            self._logger.warning('timeout must be an integer')
            return

        self._timeout = seconds

    @property
    def last_request_url(self):
        return self._request_url

    @property
    def last_response(self):
        return self._response

    @property
    def last_status_code(self):
        return self._status_code

    @property
    def last_reason(self):
        return self._reason

    @property
    def instruments(self):
        return self._instruments

    @property
    def streams(self):
        return self._streams

    @property
    def toc(self):
        return self._toc

    def fetch_table_of_contents(self):

        toc = self.build_and_send_request(12576, 'sensor/inv/toc')
        if self.last_status_code != HTTP_STATUS_OK:
            self._logger.error('Failed to create instruments list')
            return

        # Save the table of contents if we fetch it
        self._toc = toc

        return True

    def fetch_subsites(self):
        """Fetch all registered subsites from the /sensor/inv API endpoint"""

        self._logger.debug('Fetching sensor subsites')

        port = 12576
        end_point = '/sensor/inv'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_deployment_subsites(self):
        """Fetch all registered subsites from the /events/deployment/inv API
        endpoint"""

        self._logger.debug('Fetching deployment subsites')

        port = 12587
        end_point = '/events/deployment/inv'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_instrument_streams(self, ref_des):
        """Fetch all streams produced by the fully-qualified reference designator"""

        self._logger.debug('Fetching {:s} streams'.format(ref_des))

        r_tokens = ref_des.split('-')
        if len(r_tokens) != 4:
            self._logger.error(
                'Incomplete reference designator specified {:s}'.format(
                    ref_des))
            return None

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata/times'.format(
            r_tokens[0],
            r_tokens[1],
            r_tokens[2],
            r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return []

    def fetch_instrument_parameters(self, ref_des):
        """Fetch all parameters in the streams produced by the fully-qualified
        reference designator"""

        self._logger.debug(
            '{:s} - Fetching instrument parameters'.format(ref_des))

        r_tokens = ref_des.split('-')

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata/parameters'.format(
            r_tokens[0],
            r_tokens[1],
            r_tokens[2],
            r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_instrument_metadata(self, ref_des):
        """Fetch all streams and all parameters produced by the fully-qualified
        reference designator"""

        self._logger.debug(
            '{:s} - Fetching instrument metadata'.format(ref_des))

        r_tokens = ref_des.split('-')
        if len(r_tokens) != 4:
            self._logger.error(
                'Incomplete reference designator specified {:s}'.format(
                    ref_des))
            return None

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata'.format(
            r_tokens[0],
            r_tokens[1],
            r_tokens[2],
            r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_instrument_deployments(self, ref_des):
        """Fetch all deployment events for the fully or partially qualified reference designator"""

        self._logger.debug('Fetching {:s} deployments'.format(ref_des))

        port = 12587
        end_point = '/events/deployment/query?refdes={:s}'.format(ref_des)

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_stream_metadata(self, stream_name):
        """
        Fetch stream information given its name.

        Args:
            stream_name:

        Returns:

        """
        self._logger.debug(f'{stream_name} - Fetching stream metadata')

        port = 12575
        end_point = f'/stream/byname/{stream_name}'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def filter_deployments_by_status(self, deployments, status='all'):

        if status not in DEPLOYMENT_STATUS_TYPES:
            self._logger.error(
                'Invalid deployment status type specified {:s}'.format(status))
            return

        filtered_deployments = []

        if status == 'all':
            return deployments

        now = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        if status == 'active':
            for d in deployments:
                if not d['eventStopTime']:
                    filtered_deployments.append(d)
                    continue

                dt1 = datetime.datetime.utcfromtimestamp(
                    d['eventStopTime'] / 1000).replace(tzinfo=pytz.UTC)
                if dt1 >= now:
                    filtered_deployments.append(d)
        else:
            for d in deployments:
                if not d['eventStopTime']:
                    continue

                dt1 = datetime.datetime.utcfromtimestamp(
                    d['eventStopTime'] / 1000).replace(tzinfo=pytz.UTC)
                if dt1 < now:
                    filtered_deployments.append(d)

        return filtered_deployments

    def fetch_parameters(self):
        """Fetch all parameters from the /parameter API endpoint"""

        self._logger.debug('Fetching all OOI Parameters')

        port = 12575
        end_point = '/parameter'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_streams(self):
        """Fetch all streams from the /stream API endpoint"""

        self._logger.debug('Fetching all OOI Streams')

        port = 12575
        end_point = '/stream'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def fetch_vocabs(self):
        """Fetch all vocabs from the /vocab API endpoint"""

        self._logger.debug('Fetching all OOI Vocab')

        port = 12586
        end_point = '/vocab'

        request_url = self.build_request(port,
                                         end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def search_instruments(self, ref_des):
        """Search all instruments for the fully-qualified reference designators
        matching the fully or partially-qualified ref_des string"""

        return [i for i in self._instruments if i.find(ref_des) > -1]

    def stream_to_instruments(self, stream):
        """Return the list of instruments that produce the specified full or partial
        stream name"""

        return [i for i in self._instrument_streams if
                i['stream'].find(stream) > -1]

    def _use_existing_credentials(self):

        if os.path.exists(CREDENTIALS_FILE):
            import json
            with open(CREDENTIALS_FILE) as f:
                creds = json.load(f)['ooi']
                self._api_username = creds['username']
                self._api_token = creds['api_key']
        else:
            self._logger.error('Please authenticate by using yodapy.utils.set_ooi_credentials_file!')  # noqa

    def build_and_send_request(self, port, end_point):
        """Build and send the request url for the specified port and end_point"""

        request_url = self.build_request(port, end_point)

        # Send the request
        self.send_request(request_url)

        if self._status_code == HTTP_STATUS_OK:
            return self._response
        else:
            return None

    def build_request(self, port, end_point):
        """Build the request url for the specified port and end_point"""

        if self._is_m2m:
            url = '{:s}/{:0.0f}/{:s}'.format(self._m2m_base_url, port,
                                             end_point.strip('/'))
        else:
            url = '{:s}:{:0.0f}/{:s}'.format(self._base_url, port,
                                             end_point.strip('/'))

        return url

    def send_request(self, url):
        """Send the request url through either the m2m API or directly to UFrame.
        The method used is determined by the is_m2m property.  If set to True, the
        request is sent through the m2m API.  If set to False, the request is sent
        directly to UFrame"""

        # if not self._valid_uframe:
        #    self._logger.critical('Unable to connect to UFrame instance')
        #    return

        self._request_url = url
        self._response = None
        self._status_code = None
        self._reason = None
        self._response_headers = None

        if self.is_m2m and not url.startswith(self.m2m_base_url):
            self._logger.error(
                'URL does not point to the m2m base url ({:s})'.format(
                    self.m2m_base_url))
            return
        elif not url.startswith(self.base_url):
            self._logger.error(
                'URL does not point to the base url ({:s})'.format(
                    self.base_url))
            return

        try:
            self._logger.debug('Sending GET request: {:s}'.format(url))
            if self._api_username and self._api_token:
                r = self._session.get(url,
                                      auth=(self._api_username,
                                            self._api_token),
                                      timeout=self._timeout,
                                      verify=False)
            else:
                r = self._session.get(url, timeout=self._timeout, verify=False)
        except (
        requests.exceptions.ReadTimeout, requests.exceptions.MissingSchema,
        requests.exceptions.ConnectionError) as e:
            self._logger.error('{:} - {:s}'.format(e, url))
            return

        self._status_code = r.status_code
        self._reason = r.reason
        if self._status_code == HTTP_STATUS_NOT_FOUND:
            self._logger.warning('{:s}: {:s}'.format(r.reason, url))
        elif self._status_code != HTTP_STATUS_OK:
            self._logger.error(
                'Request failed {:s} ({:s})'.format(url, r.reason))

        self._response_headers = r.headers

        try:
            self._response = r.json()
            # Return the json response if there was one
            return self._response
        except ValueError as e:
            self._logger.warning('{:} ({:s})'.format(e, url))
            self._response = r.text
            return None

    def _create_parameter_index(self):
        # Get parameters
        self._logger.debug('--- Get Parameters ---')
        all_params = self.fetch_parameters()
        paramdf = pd.DataFrame.from_records(all_params)
        paramdf.loc[:, 'data_product_type'] = paramdf.apply(lambda row: get_value(row['data_product_type']), axis=1)
        productsdf = paramdf[['id', 'name', 'display_name', 'data_product_type']]
        productsdf.columns = ['parameterid', 'parameter_name', 'display_name', 'data_product_type']

        # Get streams
        self._logger.debug('--- Get Streams ---')
        all_stream = self.fetch_streams()
        streamsdf = pd.DataFrame.from_records(all_stream)
        streamsdf.loc[:, 'stream_type'] = streamsdf.apply(lambda row: get_value(row['stream_type']), axis=1)
        streamsdf.loc[:, 'stream_content'] = streamsdf.apply(lambda row: get_value(row['stream_content']), axis=1)
        inststreams = streamsdf[['id', 'name', 'parameters', 'stream_content', 'time_parameter', 'stream_type']]

        # -- Expand parameters in science streams
        science_streams = inststreams.copy()
        science_streams.loc[:, 'parameterid'] = science_streams.parameters.apply(lambda params: [p['id'] for p in params])
        science_streams = split_val_list(science_streams, 'parameterid')
        science_streams.drop('parameters', axis=1, inplace=True)
        science_streams.columns = ['streamid', 'name', 'stream_content', 'time_parameter', 'stream_type', 'parameterid']

        joined_params = pd.merge(productsdf, science_streams, on='parameterid')

        # Get instruments
        self._logger.debug('--- Get Instruments ---')
        sensor_inv = self._toc
        instrumentsdf = pd.DataFrame.from_records(sensor_inv['instruments'])
        instdf = split_val_list(instrumentsdf, 'streams')
        instdf.loc[:, 'stream_name'] = instdf.apply(lambda row: row['streams']['stream'], axis=1)
        instdf.loc[:, 'stream_method'] = instdf.apply(lambda row: row['streams']['method'], axis=1)
        instdf.drop('streams', axis=1, inplace=True)
        all_instruments = pd.merge(joined_params, instdf, left_on='name', right_on='stream_name').reset_index(drop='index')

        # Get Instrument metada
        self._logger.info('--- Get Instrument Metadata ---')
        vocabs = self.fetch_vocabs()
        vocabdf = pd.DataFrame.from_records(vocabs)
        all_vocabs = vocabdf[['vocabId', 'instrument', 'manufacturer', 'refdes', 'tocL1', 'tocL2', 'tocL3']]
        es_index = pd.merge(all_instruments, all_vocabs, left_on='reference_designator', right_on='refdes')
        es_index.drop(['name', 'refdes'], axis=1, inplace=True)
        column_array = ['parameter_id',
                        'parameter_name',
                        'parameter_display_name',
                        'parameter_product_type',
                        'stream_id',
                        'stream_content',
                        'time_parameter',
                        'stream_type',
                        'instrument_code',
                        'node_code',
                        'site_code',
                        'instrument_refdes',
                        'stream_name',
                        'stream_method',
                        'vocabid',
                        'instrument_name',
                        'instrument_manufacturer',
                        'array_name',
                        'site_name',
                        'node_name']
        es_index.columns = column_array
        self._logger.debug('--- Exporting es_index ---')
        self._parameter_index = es_index.reset_index(drop='index')

    def _create_instrument_list(self):

        self._instruments = []
        self._streams = []
        self._instrument_streams = []

        self._logger.debug('Fetching UFrame table of contents')
        if not self._toc:
            self.fetch_table_of_contents()

        self._logger.debug('Creating instruments list')

        # Create an array of dicts with the instrument name and the stream it produces
        self._instrument_streams = [
            {'instrument': i['reference_designator'], 'stream': s['stream']}
            for i in self._toc['instruments'] for s in i['streams']]

        # Create the unique list of streams
        streams = list(set([i['stream'] for i in self._instrument_streams]))
        streams.sort()
        self._streams = streams

        # Create the unique list of instruments
        instruments = list(
            set([i['instrument'] for i in self._instrument_streams]))
        instruments.sort()
        self._instruments = instruments

    def instrument_to_query(self, ref_des, user, stream=None, telemetry=None,
                            time_delta_type=None,
                            time_delta_value=None, begin_ts=None, end_ts=None,
                            time_check=True, exec_dpa=True,
                            application_type='netcdf', provenance=True,
                            limit=-1, annotations=False, email=None):
        """Return the list of request urls that conform to the UFrame API for the specified
        fully or paritally-qualified reference_designator.  Request urls are formatted
        for either the UFrame m2m API (default) or direct UFrame access, depending
        on is_m2m property of the UFrameClient instance.

        Arguments:
            ref_des: partial or fully-qualified reference designator
            stream: restrict urls to the specified stream
            user: user name for the query

        Optional kwargs:
            telemetry: telemetry type (Default is all telemetry types
            time_delta_type: Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a
                type kwarg accepted by dateutil.relativedelta'
            time_delta_value: Positive integer value to subtract from the end time to get the start time for subsetting.
            begin_ts: ISO-8601 formatted datestring specifying the dataset start time
            end_ts: ISO-8601 formatted datestring specifying the dataset end time
            time_check: set to true (default) to ensure the request times fall within the stream data availability
            exec_dpa: boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters
                (Default is True)
            application_type: 'netcdf' or 'json' (Default is 'netcdf')
            provenance: boolean value specifying whether provenance information should be included in the data set
                (Default is True)
            limit: integer value ranging from -1 to 10000.  A value of -1 (default) results in a non-decimated dataset
            annotations: boolean value (True or False) specifying whether to include all dataset annotations
        """
        urls = []

        application_type = application_type.lower()
        if application_type not in ['netcdf', 'json']:
            text = f'Invalid application type/format: {application_type}'
            self._logger.error(text)
            raise ValueError(text)

        if application_type == 'json':
            if limit <= 0:
                text = 'Please specify data points limit!'
                self._logger.error(text)
                raise ValueError(text)

        instruments = self.search_instruments(ref_des)
        if not instruments:
            return urls

        if time_delta_type and time_delta_value:
            if time_delta_type not in _valid_relativedeltatypes:
                text = f'Invalid datetutil.relativedelta type: {time_delta_type}'
                self._logger.error(text)
                raise ValueError(text)

        begin_dt = None
        end_dt = None
        if begin_ts:
            try:
                begin_dt = parser.parse(begin_ts).replace(tzinfo=pytz.UTC)
            except ValueError as e:
                self._logger.error(f'Invalid begin_dt: {begin_ts} ({e})')
                return urls

        if end_ts:
            try:
                end_dt = parser.parse(end_ts).replace(tzinfo=pytz.UTC)
            except ValueError as e:
                self._logger.error(
                    'Invalid end_dt: {:s} ({:s})'.format(end_ts, e.message))
                return urls

        for instrument in instruments:

            # Get the streams produced by this instrument
            instrument_streams = self.fetch_instrument_streams(instrument)
            if not instrument_streams:
                self._logger.info(
                    'No streams found for {:s}'.format(instrument))
                continue

            if stream:
                stream_names = [s['stream'] for s in instrument_streams]
                if stream not in stream_names:
                    self._logger.warning(
                        'Invalid stream: {:s}-{:s}'.format(instrument, stream))
                    continue

                instrument_streams = [s for s in instrument_streams if
                                      s['stream'] == stream]
            #                i = stream_names.index(stream)
            #                instrument_streams = [instrument_streams[i]]

            if not instrument_streams:
                self._logger.info('{:s}: No streams found'.format(instrument))
                continue

            # Break the reference designator up
            r_tokens = instrument.split('-')

            for instrument_stream in instrument_streams:

                if telemetry and not instrument_stream['method'].startswith(
                        telemetry):
                    continue

                # Figure out what we're doing for time
                try:
                    stream_dt0 = parser.parse(instrument_stream['beginTime'])
                except ValueError:
                    self._logger.error(
                        '{:s}-{:s}: Invalid beginTime ({:s})'.format(
                            instrument, instrument_stream['stream'],
                            instrument_stream['beginTime']))
                    continue

                try:
                    stream_dt1 = parser.parse(instrument_stream['endTime'])
                    # Add 1 second to stream end time to account for milliseconds
                    stream_dt1 = stream_dt1 + tdelta(seconds=1)
                except ValueError:
                    self._logger.error(
                        '{:s}-{:s}: Invalid endTime ({:s})'.format(
                            'instrument', instrument_stream['stream'],
                            instrument_stream['endTime']))
                    continue

                if time_delta_type and time_delta_value:
                    dt1 = stream_dt1
                    dt0 = dt1 - tdelta(
                        **dict({time_delta_type: time_delta_value}))
                else:
                    if begin_dt:
                        dt0 = begin_dt
                    else:
                        dt0 = stream_dt0

                    if end_dt:
                        dt1 = end_dt
                    else:
                        dt1 = stream_dt1

                # Format the endDT and beginDT values for the query
                try:
                    ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument,
                                                                instrument_stream[
                                                                    'stream'],
                                                                e.message))
                    continue

                try:
                    ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument,
                                                                instrument_stream[
                                                                    'stream'],
                                                                e.message))
                    continue

                # Make sure the specified or calculated start and end time are within
                # the stream metadata times if time_check=True
                if time_check:
                    if dt1 > stream_dt1:
                        self._logger.warning(
                            '{:s}-{:s} time check - End time exceeds stream endTime'.format(
                                ref_des, instrument_stream['stream']))
                        self._logger.warning(
                            '{:s}-{:s} time check - Setting request end time to stream endTime'.format(
                                ref_des, instrument_stream['stream']))
                        ts1 = instrument_stream['endTime']

                    if dt0 < stream_dt0:
                        self._logger.warning(
                            '{:s}-{:s} time check - Start time is earlier than stream beginTime'.format(
                                ref_des, instrument_stream['stream']))
                        self._logger.warning(
                            '{:s}-{:s} time check -  Setting request begin time to stream beginTime'.format(
                                ref_des, instrument_stream['stream']))
                        ts0 = instrument_stream['beginTime']

                    # Check that ts0 < ts1
                    dt0 = parser.parse(ts0)
                    dt1 = parser.parse(ts1)
                    if dt0 >= dt1:
                        self._logger.warning(
                            '{:s}-{:s} - Invalid time range specified'.format(
                                instrument, instrument_stream['stream']))
                        continue

                # Create the url
                end_point = 'sensor/inv/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&limit={:d}&execDPA={:s}&include_provenance={:s}&user={:s}'.format(
                    r_tokens[0],
                    r_tokens[1],
                    r_tokens[2],
                    r_tokens[3],
                    instrument_stream['method'],
                    instrument_stream['stream'],
                    ts0,
                    ts1,
                    application_type,
                    limit,
                    str(exec_dpa).lower(),
                    str(provenance).lower(),
                    user)

                if email:
                    end_point = '{:s}&email={:s}'.format(end_point, email)

                urls.append(self.build_request(12576, end_point))

        return urls

    def __repr__(self):
        return '<UFrameClient(url={:s}, m2m={:s})>'.format(self.base_url,
                                                           str(self._is_m2m))