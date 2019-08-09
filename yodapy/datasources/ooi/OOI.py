""" OOI Object """

import os
import datetime
from io import StringIO
import re
from queue import Queue

import gevent

import pandas as pd

import requests

import threading
import time
import logging
import warnings

from lxml.html import fromstring as html_parser

from dateutil import parser
import pytz
import s3fs
import urllib3
import xarray as xr

from yodapy.datasources.ooi.CAVA import CAVA

from yodapy.utils.files import CREDENTIALS_FILE
from yodapy.datasources.ooi.helpers import set_thread
from yodapy.utils.conn import (fetch_url,
                               instrument_to_query,
                               fetch_xr,
                               get_download_urls,
                               download_url,
                               perform_ek60_download,
                               perform_ek60_processing)
from yodapy.utils.parser import (parse_toc_instruments,
                                 parse_streams_dataframe,
                                 parse_raw_data_catalog,
                                 parse_parameter_streams_dataframe,
                                 parse_global_range_dataframe,
                                 parse_deployments_json,
                                 parse_annotations_json,
                                 get_instrument_list,
                                 unix_time_millis,
                                 get_nc_urls)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-10s) %(message)s',
                    )

logger = logging.getLogger(__name__)

print_lock = threading.Lock()

DATA_TEAM_GITHUB_INFRASTRUCTURE = 'https://raw.githubusercontent.com/ooi-data-review/datateam-portal-backend/master/infrastructure'
FILE_SYSTEM = s3fs.S3FileSystem(anon=True)
BUCKET_DATA = 'io2data/data'


class OOI(CAVA):
    """OOI Object for Ocean Observatories Initiative Data Retrieval.

    Attributes:
        ooi_name (str): Username for OOI API Data Access.
        ooi_token (str): Token for OOI API Data Access.
        source_name (str): Data source name.
        regions (pandas.DataFrame): Table of OOI regions.
        sites (pandas.DataFrame): Table of OOI sites.
        instruments (pandas.DataFrame): Table of available instrument streams.
        global_ranges (pandas.DataFrame): Table of global ranges for each instrument streams.
        deployments (pandas.DataFrame): Table of deployments for filtered instrument streams.
        annotations (pandas.DataFrame): Table of annotations for filtered instrument streams.
        start_date (list): List of start dates requested.
        end_date (list): List of end dates requested.
        last_request (list): List of requested urls and parameters.
        last_m2m_urls (list): List of requested M2M urls.

        cava_arrays (pandas.DataFrame): Cabled array team Arrays vocab table.
        cava_sites (pandas.DataFrame): Cabled array team Sites vocab table.
        cava_infrastructures (pandas.DataFrame): Cabled array team Infrastructures vocab table.
        cava_instruments (pandas.DataFrame): Cabled array team Instruments vocab table.
        cava_parameters (pandas.DataFrame): Cabled array team Parameters vocab table.


    """

    def __init__(self,
                 ooi_username=None,
                 ooi_token=None,
                 cloud_source=False,
                 **kwargs):
        super().__init__()
        self._source_name = 'OOI'
        self._start_date = None
        self._end_date = None
        # Private global variables
        self._OOI_M2M_VOCAB = 'https://ooinet.oceanobservatories.org/api/m2m/12586/vocab'
        self._OOI_M2M_TOC = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/toc'
        self._OOI_M2M_STREAMS = 'https://ooinet.oceanobservatories.org/api/m2m/12575/stream'
        self._OOI_DATA_URL = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv'
        self._OOI_M2M_ANNOTATIONS = 'https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find'
        self._OOI_M2M_DEPLOYMENT_QUERY = 'https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/query'

        # From visualocean
        self._OOI_VISUALOCEAN_M_STATS = 'https://ooi-visualocean.whoi.edu/instruments/stats-monthly'

        self._OOI_GLOBAL_RANGE = 'https://raw.githubusercontent.com/ooi-integration/qc-lookup/master/data_qc_global_range_values.csv'

        # From GitHub
        self._OOI_PORTAL_REGIONS = f'{DATA_TEAM_GITHUB_INFRASTRUCTURE}/regions.csv'
        self._OOI_PORTAL_SITES = f'{DATA_TEAM_GITHUB_INFRASTRUCTURE}/sites.csv'
        # Not used
        # self._OOI_VOCAB = 'https://raw.githubusercontent.com/ooi-integration/asset-management/master/vocab/vocab.csv'

        self._regions = None
        self._sites = None

        # User inputs
        self.ooi_username = ooi_username
        self.ooi_token = ooi_token

        # Private cache variables
        self._rvocab = None
        self._rglobal_range = None
        self._rstreams = None
        self._rtoc = None

        self._raw_datadf = None
        self._raw_data_url = None

        # For bio-acoustic sonar
        self._zplsc_data_catalog = None
        self._raw_file_dict = None

        self._data_type = None

        self._current_data_catalog = None
        self._filtered_data_catalog = None

        self._q = None
        self._raw_data = []
        self._dataset_list = []
        self._netcdf_urls = []

        # Cloud copy
        self._s3content = None
        self._cloud_source = cloud_source
        # ----------- Session Configs ---------------------
        self._session = requests.Session()
        self._pool_connections = kwargs.get('pool_connections', 100)
        self._pool_maxsize = kwargs.get('pool_maxsize', 100)
        self._adapter = requests.adapters.HTTPAdapter(
            pool_connections=self._pool_connections,
            pool_maxsize=self._pool_maxsize)
        self._session.mount('https://', self._adapter)
        self._session.verify = False
        # --------------------------------------------------

        self._request_urls = None
        self._last_m2m_urls = []
        self._last_download_list = None
        self._last_downloaded_netcdfs = None
        self._thread_list = []

        self._setup()

    @property
    def regions(self):
        """ Returns the OOI regions """
        if not isinstance(self._regions, pd.DataFrame):
            try:
                self._regions = pd.read_csv(self._OOI_PORTAL_REGIONS).rename({
                    'reference_designator': 'array_rd',
                    'name': 'region_name'
                }, axis='columns')
            except Exception as e:
                logger.error(e)
        return self._regions

    @property
    def sites(self):
        """ Returns the OOI sites """
        if not isinstance(self._sites, pd.DataFrame):
            try:
                self._sites = pd.read_csv(self._OOI_PORTAL_SITES).dropna(subset=[  # noqa
                    'longitude', 'latitude'
                ]).rename({
                    'reference_designator': 'site_rd',
                    'name': 'site_name'
                }, axis='columns')
            except Exception as e:
                logger.error(e)
        return self._sites

    @property
    def instruments(self):

        def threads_alive(t):
            return not t.is_alive()
        if all(list(map(threads_alive, self._thread_list))):
            """ Returns instruments dataframe """
            if isinstance(self._filtered_data_catalog, pd.DataFrame):
                return get_instrument_list(self._filtered_data_catalog)
            if isinstance(self._current_data_catalog, pd.DataFrame):
                return get_instrument_list(self._current_data_catalog)
        else:
            message = 'Please wait while we fetch the metadata ...'
            logger.info(message)

    @property
    def deployments(self):
        """ Return instruments deployments """
        instrument_list = self._current_data_catalog
        if isinstance(self._filtered_data_catalog, pd.DataFrame):
            instrument_list = self._filtered_data_catalog

        if len(instrument_list) <= 50:
            text = f'Fetching deployments from {len(instrument_list)} unique instrument streams...'  # noqa
            print(text)  # noqa
            logger.info(text)

            dflist = [self._get_deployments(inst) for idx, inst in instrument_list.iterrows()]  # noqa
            return pd.concat(dflist).reset_index(drop='index')
        else:
            raise Exception(f'You have {len(instrument_list)} unique streams; too many to fetch deployments. Please filter by performing search.')  # noqa

    @property
    def annotations(self):
        """ Return instruments annotations """
        instrument_list = self._current_data_catalog
        if isinstance(self._filtered_data_catalog, pd.DataFrame):
            instrument_list = self._filtered_data_catalog

        if len(instrument_list) <= 20:
            text = f'Fetching annotations from {len(instrument_list)} unique instrument streams...'  # noqa
            print(text)  # noqa
            logger.info(text)

            dflist = [self._get_annotations(inst) for idx, inst in instrument_list.iterrows()]  # noqa
            return pd.concat(dflist).reset_index(drop='index')
        else:
            raise Exception(f'You have {len(instrument_list)} unique streams; too many to fetch annotations. Please filter by performing search.')  # noqa

    @property
    def start_date(self):
        """ Return requested start date(s) """
        if isinstance(self._start_date, pd.Series):
            return self._start_date
        return 'Start date(s) can\'t be found.'

    @property
    def end_date(self):
        """ Return requested end date(s) """
        if isinstance(self._end_date, pd.Series):
            return self._end_date
        return 'End date(s) can\'t be found.'

    @property
    def source_name(self):
        """ Return data source name """
        return self._source_name

    @property
    def last_requests(self):
        """ Return last request url and parameters """
        if self._request_urls:
            return self._request_urls
        return 'Data request has not been made.'

    @property
    def last_m2m_urls(self):
        """ Return last request m2m urls """
        if self._last_m2m_urls:
            return self._last_m2m_urls
        return 'Data request has not been made.'

    @property
    def global_ranges(self):
        """ Return global ranges """
        return self._get_global_ranges()

    def view_instruments(self):
        """
        **DEPRECATED.** 
        Shows the current instruments requested.
        Use OOI.instruments attribute instead.

        Returns:
            DataFrame: Pandas dataframe of the instruments.

        """
        warnings.warn('The function view_instruments is deprecated. Please use OOI.instruments attribute instead.',
                      DeprecationWarning, stacklevel=2)
        return self.instruments

    def view_regions(self):
        """
        **DEPRECATED.**
        Shows the regions within OOI.
        Use OOI.regions attribute instead.

        Returns:
            DataFrame: Pandas dataframe of the regions.

        """
        warnings.warn('The function view_regions is deprecated. Please use OOI.regions attribute instead.',
                      DeprecationWarning, stacklevel=2)
        return self.regions

    def view_sites(self):
        """
        **DEPRECATED.**
        Shows the sites within OOI.
        Use OOI.sites attribute instead.

        Returns:
            DataFrame: Pandas dataframe of the sites.

        """
        warnings.warn('The function view_sites is deprecated. Please use OOI.sites attribute instead.',
                      DeprecationWarning, stacklevel=2)
        return self.sites

    def __repr__(self):
        """ Prints out the representation of the OOI object """
        inst_text = 'Instrument Stream'
        if isinstance(self._current_data_catalog, pd.DataFrame):
            data_length = len(self._current_data_catalog.drop_duplicates(
                subset=['reference_designator',
                        'stream_method',
                        'stream_rd']))
        else:
            data_length = 0

        if isinstance(self._filtered_data_catalog, pd.DataFrame):
            data_length = len(self._filtered_data_catalog.drop_duplicates(
                subset=['reference_designator',
                        'stream_method',
                        'stream_rd']))
        if data_length > 1:
            inst_text = inst_text + 's'

        return f'<Data Source: {self._source_name} ({data_length} {inst_text})>'  # noqa

    def __len__(self):
        """ Prints the length of the object """
        if isinstance(self._filtered_data_catalog, pd.DataFrame):
            return len(self._filtered_data_catalog.drop_duplicates(
                subset=['reference_designator',
                        'stream_method',
                        'stream_rd']
            ))
        else:
            return 0

    def _setup(self):
        """ Setup the OOI Instance by fetching data catalog ahead of time """
        logger.debug('Setting UFrame credentials.')
        if not self.ooi_username or not self.ooi_token:
            self._use_existing_credentials()

        # Check if ooinet is available
        try:
            req = requests.get('https://ooinet.oceanobservatories.org')

            if req.status_code == 200:
                threads = [('get-data-catalog', self._get_data_catalog),
                           ('get-global-ranges', self._get_global_ranges),
                           ('get-rawdata-filelist', self._get_rawdata_filelist)]  # noqa
                for t in threads:
                    ft = set_thread(*t)
                    self._thread_list.append(ft)
            else:
                logger.warning(
                    f'Server not available, please try again later: {req.status_code}')
        except Exception as e:
            logger.error(f'Server not available, please try again later: {e}')

        # Retrieve datasets info in the s3 bucket.
        try:
            self._s3content = [os.path.basename(
                rd) for rd in FILE_SYSTEM.ls(BUCKET_DATA)]
        except Exception as e:
            logger.error(e)

    def request_data(self, begin_date, end_date,
                     data_type='netcdf', limit=-1, **kwargs):
        """
        Request data for filtered instruments.

        Args:
            begin_date (str): Begin date of desired data in ISO-8601 Format.
            end_date (str): End date of desired data in ISO-8601 Format.
            data_type (str): Desired data type. Either 'netcdf' or 'json'.
            limit (int, optional): Desired data points. Required for 'json' ``data_type``. Max is 20000.
            **kwargs: Optional Keyword arguments. \n
                **time_check** - set to true (default) to ensure the request times fall within the stream data availability \n
                **exec_dpa** - boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters (Default is True) \n
                **provenance** - boolean value specifying whether provenance information should be included in the data set (Default is True) \n
                **email** - provide email.
        Returns:
            self: Modified OOI Object. Use ``raw()`` to see either data url for netcdf or json result for json.

        """

        self._data_type = data_type
        begin_dates = list(map(lambda x: x.strip(' '), begin_date.split(',')))
        end_dates = list(map(lambda x: x.strip(' '), end_date.split(',')))

        data_catalog_copy = self._filtered_data_catalog.copy()

        self._q = Queue()

        # Limit the number of request
        if len(data_catalog_copy) > 6:
            text = f'Too many instruments to request data for! Max is 6, you have {len(data_catalog_copy)}'  # noqa
            logger.error(text)
            raise Exception(text)

        if len(begin_dates) == 1 and len(end_dates) == 1:
            begin_dates = begin_dates[0]
            end_dates = end_dates[0]
        elif len(begin_dates) != len(end_dates):
            logger.warning(
                'Please provide the same number of begin and end dates')
            raise ValueError(
                'Please provide the same number of begin and end dates')
        else:
            begin_dates = pd.Series(begin_dates)
            end_dates = pd.Series(end_dates)

        self._start_date = begin_date,
        self._end_date = end_dates

        request_urls = []
        if self._cloud_source:
            data_catalog_copy.loc[:, 'user_begin'] = pd.to_datetime(
                begin_dates)
            data_catalog_copy.loc[:, 'user_end'] = pd.to_datetime(end_dates)
            data_catalog_copy.loc[:, 'full_rd'] = data_catalog_copy.apply(
                lambda row: '-'.join([row['reference_designator'], row['stream_method'], row['stream_rd']]), axis=1)
            data_catalog_copy.loc[:, 'rd_path'] = data_catalog_copy['full_rd'].apply(
                lambda row: '/'.join([BUCKET_DATA, row]))
            request_urls = data_catalog_copy['rd_path'].values.tolist()

            for idx, row in data_catalog_copy.iterrows():
                tempdf = pd.DataFrame(FILE_SYSTEM.ls(
                    row['rd_path']), columns=['uri'])
                tempdf.loc[:, 'time'] = tempdf.apply(
                    lambda r: pd.to_datetime(os.path.basename(r['uri'])), axis=1)
                selected = tempdf[(tempdf.time >= row['user_begin']) & (
                    tempdf.time <= row['user_end'])]
                if len(selected) > 0:
                    self._q.put([selected, row['user_begin'], row['user_end']])

        else:
            data_catalog_copy['user_begin'] = begin_dates
            data_catalog_copy['user_end'] = end_dates
            # For bio-acoustic sonar only
            self._zplsc_data_catalog = data_catalog_copy[data_catalog_copy.instrument_name.str.contains(
                'bio-acoustic sonar', case=False)]
            data_catalog_copy = data_catalog_copy[~data_catalog_copy.instrument_name.str.contains(
                'bio-acoustic sonar', case=False)]
            if len(data_catalog_copy) > 0:
                request_urls = [instrument_to_query(
                    ooi_url=self._OOI_DATA_URL,
                    site_rd=row.site_rd,
                    infrastructure_rd=row.infrastructure_rd,
                    instrument_rd=row.instrument_rd,
                    stream_method=row.stream_method,
                    stream_rd=row.stream_rd,
                    begin_ts=row.user_begin,
                    end_ts=row.user_end,
                    stream_start=row.begin_date,
                    stream_end=row.end_date,
                    application_type=data_type,
                    limit=limit,
                    **kwargs
                ) for idx, row in data_catalog_copy.iterrows()]

                prepared_requests = [requests.Request('GET',
                                                    data_url,
                                                    auth=(self.ooi_username,
                                                            self.ooi_token),
                                                    params=params) for data_url, params in request_urls]  # noqa

                for job in prepared_requests:
                    prepped = job.prepare()
                    self._last_m2m_urls.append(prepped.url)
                    self._q.put(prepped)

        if len(self._raw_data) > 0:
            self._raw_data = []
        self._process_request()

        # block until all tasks are done
        self._q.join()

        if isinstance(self._zplsc_data_catalog, pd.DataFrame):
            if len(self._zplsc_data_catalog) > 0:
                self._zplsc_data_catalog.loc[:, 'ref'] = self._zplsc_data_catalog.reference_designator.apply(
                    lambda rd: rd[:14])
                filtered_datadf = {}
                for idx, row in self._zplsc_data_catalog.iterrows():
                    filtered_datadf[row['ref']] = self._raw_datadf[row['ref']
                                                                   ][row['user_begin']:row['user_end']].copy()
                    filtered_rawdata = filtered_datadf[row['ref']]
                    filtered_rawdata.loc[:, 'urls'] = filtered_rawdata.filename.apply(
                        lambda f: '/'.join([self._raw_data_url[row['ref']], f]))

                raw_file_dict = perform_ek60_download(filtered_datadf)
                self._raw_file_dict = raw_file_dict
                self._raw_data.append(raw_file_dict)
        self._request_urls = request_urls
        return self

    def search(self, region=None, site=None,
               node=None, instrument=None,
               stream_type='Science',
               stream_method=None,
               stream=None,
               parameter=None):
        """
        Perform a search, and filters data catalog

        Args:
            region (str): Region name. If multiple use comma separated.
            site (str): Site name. If multiple use comma separated.
            node (str): Node name. If multiple use comma separated.
            instrument (str): Instrument name. If multiple use comma separated.
            stream_type (str): Stream type. Either 'Science' or 'Engineering'. If multiple use comma separated.
            stream_method (str): Stream method. If multiple use comma separated.
            stream (str): Stream name. If multiple use comma separated.
            parameter (str): Parameter name. If multiple use comma separated.

        Returns:
            self: Modified OOI Object

        """
        if isinstance(self._current_data_catalog, pd.DataFrame):
            current_dcat = self._current_data_catalog
        else:
            current_dcat = self._get_data_catalog()
            self._current_data_catalog = current_dcat

        if self._cloud_source:
            current_dcat = current_dcat[current_dcat.apply(lambda row: '-'.join(
                [row['reference_designator'], row['stream_method'], row['stream_rd']]) in self._s3content, axis=1)].reset_index(drop='index')

        if region:
            region_search = list(map(lambda x: x.strip(' '), region.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.array_name.astype(str).str.contains('|'.join(region_search), flags=re.IGNORECASE) | current_dcat.site_rd.astype(str).str.contains('|'.join(region_search), flags=re.IGNORECASE) | current_dcat.reference_designator.astype(str).str.contains('|'.join(region_search), flags=re.IGNORECASE)]  # noqa
        if site:
            site_search = list(map(lambda x: x.strip(' '), site.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.site_name.astype(str).str.contains('|'.join(site_search), flags=re.IGNORECASE) | current_dcat.site_rd.astype(str).str.contains('|'.join(site_search), flags=re.IGNORECASE) | current_dcat.reference_designator.astype(str).str.contains('|'.join(site_search), flags=re.IGNORECASE)]  # noqa
        if node:
            node_search = list(map(lambda x: x.strip(' '), node.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.infrastructure_name.astype(str).str.contains('|'.join(node_search), flags=re.IGNORECASE) | current_dcat.infrastructure_rd.astype(str).str.contains('|'.join(node_search), flags=re.IGNORECASE) | current_dcat.reference_designator.astype(str).str.contains('|'.join(node_search), flags=re.IGNORECASE)]  # noqa
        if instrument:
            instrument_search = list(map(lambda x: x.strip(' '), instrument.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.instrument_name.astype(str).str.contains('|'.join(instrument_search), flags=re.IGNORECASE) | current_dcat.instrument_rd.astype(str).str.contains('|'.join(instrument_search), flags=re.IGNORECASE) | current_dcat.reference_designator.astype(str).str.contains('|'.join(instrument_search), flags=re.IGNORECASE)]  # noqa
        if parameter:
            parameter_search = list(map(lambda x: x.strip(' '), parameter.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.display_name.astype(str).str.contains('|'.join(parameter_search), flags=re.IGNORECASE) | current_dcat.parameter_rd.astype(str).str.contains('|'.join(parameter_search), flags=re.IGNORECASE)]  # noqa
        if stream_type:
            stream_type_search = list(map(lambda x: x.strip(' '), stream_type.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.stream_type.astype(str).str.contains('|'.join(stream_type_search), flags=re.IGNORECASE)]  # noqa
        if stream_method:
            stream_method_search = list(map(lambda x: x.strip(' '), stream_method.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.stream_method.astype(str).str.contains('|'.join(stream_method_search), flags=re.IGNORECASE)]  # noqa
        if stream:
            stream_search = list(map(lambda x: x.strip(' '), stream.split(',')))  # noqa
            current_dcat = current_dcat[current_dcat.stream_rd.astype(str).str.contains('|'.join(stream_search), flags=re.IGNORECASE)]  # noqa
        self._filtered_data_catalog = current_dcat.drop_duplicates(
            subset=['reference_designator',
                    'stream_method',
                    'stream_rd']
        )[['array_name',
           'site_name',
           'infrastructure_name',
           'instrument_name',
           'site_rd',
           'infrastructure_rd',
           'instrument_rd',
           'reference_designator',
           'stream_method',
           'stream_type',
           'stream_rd',
           'begin_date',
           'end_date']].reset_index(drop='index')

        return self

    def clear(self):
        """
        Clears the search filter.

        Returns:
            self: Modified OOI Object
        """
        if isinstance(self._filtered_data_catalog, pd.DataFrame):
            self._filtered_data_catalog = None
        return self

    def raw(self):
        """ Returns the raw result from data request in json format """
        return self._raw_data

    def download_netcdfs(self, destination=os.path.curdir, timeout=3600):
        """
        Download netcdf files from the catalog created from data request.

        Args:
            destination (str, optional): Location to save netcdf file. Default will save in current directory.
            timeout (int, optional): Expected download time before timing out in seconds. Defaults to 30min or 3600s.

        Returns:
            list: List of exported netcdf.
        """
        if not isinstance(timeout, int):
            raise TypeError(f'Expected int; {type(int)} given.')

        download_list = self._prepare_download()
        logger.info('Downloading netcdfs ...')
        jobs = [gevent.spawn(download_url,
                             url,
                             destination,
                             self._session) for url in download_list]
        gevent.joinall(jobs, timeout=timeout)
        finished_netcdfs = [job.value for job in jobs]
        if finished_netcdfs:
            self._last_downloaded_netcdfs = [os.path.join(os.path.abspath(destination), nc) for nc in finished_netcdfs]  # noqa
        return self._last_downloaded_netcdfs

    def to_xarray(self, **kwargs):
        """
        Retrieve the OOI streams data and export to Xarray Datasets, saving in memory.

        Args:
            **kwargs: Keyword arguments for xarray open_mfdataset.

        Returns:
            list: List of xarray datasets
        """
        ref_degs = self._filtered_data_catalog['reference_designator'].values
        dataset_list = []
        if self._data_type == 'netcdf':
            if not self._cloud_source:
                if self._raw_file_dict:
                    mvbsnc_list = perform_ek60_processing(self._raw_file_dict)
                    for k, v in mvbsnc_list.items():
                        resdf = xr.open_mfdataset(v,
                                                  concat_dim=['ping_time'],
                                                  combine='nested',
                                                  **kwargs)
                        dataset_list.append(resdf)
                turls = self._perform_check()

                if len(turls) > 0:
                    self._netcdf_urls = [get_nc_urls(turl) for turl in turls]
                    logger.info('Acquiring data from opendap urls ...')
                    jobs = [gevent.spawn(fetch_xr, (url, ref_degs), **kwargs)
                            for url in turls]
                    gevent.joinall(jobs, timeout=300)
                    for job in jobs:
                        dataset_list.append(job.value)
        else:
            self._logger.warning(f'{self._data_type} cannot be converted to xarray dataset')  # noqa

        if dataset_list:
            self._dataset_list = dataset_list

        return self._dataset_list

    def check_status(self):
        """ Function for user to manually check the status of the data """
        if not self._q.empty():
            return None
        turls = []
        filtered_data_urls = list(filter(lambda x: 'allURLs' in x, self.raw()))
        for durl in filtered_data_urls:
            turl = self._check_data_status(durl)
            if turl:
                turls.append(turl)

        if len(turls) == len(filtered_data_urls):
            return turls
        return None

    def data_availability(self):
        """
        Plots data availability of desired instruments.

        Returns:
            pandas.DataFrame: Instrument Stream legend

        """
        import matplotlib.pyplot as plt
        import seaborn as sns

        plt.clf()
        plt.close('all')

        inst = self._filtered_data_catalog.copy()

        if isinstance(inst, pd.DataFrame):
            if len(inst) > 0:
                da_list = []
                for idx, i in inst.iterrows():
                    if i.instrument_name not in ['Bio-acoustic Sonar (Coastal)']:
                        da_list.append(self._fetch_monthly_stats(i))
                    else:
                        print(
                            f'{i.reference_designator} not available for data availability')
                if len(da_list) > 0:
                    dadf = pd.concat(da_list)
                    dadf.loc[:, 'unique_rd'] = dadf.apply(
                        lambda row: '-'.join([row.reference_designator,
                                              row.stream_method,
                                              row.stream_rd]), axis=1)

                    inst.loc[:, 'unique_rd'] = inst.apply(lambda row: '-'.join([row.reference_designator,
                                                                                row.stream_method,
                                                                                row.stream_rd]), axis=1)
                    name_df = inst[['array_name', 'site_name',
                                    'infrastructure_name', 'instrument_name',
                                    'unique_rd']]

                    raw_plotdf = pd.merge(dadf, name_df)
                    plotdf = raw_plotdf.pivot_table(
                        index='unique_rd', columns='month', values='percentage',)

                    sns.set(style='white')
                    _, ax = plt.subplots(figsize=(20, 10))

                    ax.set_title('OOI Data Availability')

                    sns.heatmap(plotdf, annot=False,
                                fmt='.2f', linewidths=1,
                                ax=ax, square=True,
                                cmap=sns.light_palette('green'),
                                cbar_kws={
                                    'orientation': 'horizontal',
                                    'shrink': 0.7,
                                    'pad': 0.3,
                                    'aspect': 30
                                })
                    plt.ylabel('Instruments', rotation=0, labelpad=60)
                    plt.xlabel('Months', labelpad=30)
                    plt.yticks(rotation=0)
                    plt.tight_layout()

                    legend = raw_plotdf[
                        (list(raw_plotdf.columns.values[-5:]) + ['stream_method',
                                                                 'stream_rd'])
                    ].drop_duplicates(subset='unique_rd')
                    legend.loc[:, 'labels'] = legend.apply(lambda row:
                                                        [row.array_name,
                                                            row.site_name,
                                                            row.infrastructure_name,  # noqa
                                                            row.instrument_name,
                                                            row.stream_method,
                                                            row.stream_rd], axis=1)
                    ldct = {}
                    for idx, row in legend.iterrows():
                        ldct[row.unique_rd] = row.labels
                    return pd.DataFrame.from_dict(ldct)
                return None

            elif len(inst) > 50:
                raise Exception(f'You have {len(inst)} unique streams; too many to fetch deployments. Please filter by performing search.')  # noqa
            else:
                logger.warning('Data catalog is empty.')
        else:
            logger.warning(
                'Please find your desired instruments by using .search() method.')
            return self

    def _perform_check(self):
        """ Performing data status check every 10 seconds """
        turls = self.check_status()
        start = datetime.datetime.now()
        while turls is None:
            time.sleep(10)
            end = datetime.datetime.now()
            delta = end - start
            logger.info(f'Data request time elapsed: {delta.seconds}s')
            print(f'Data request time elapsed: {delta.seconds}s')
            turls = self.check_status()
        return turls

    def _prepare_download(self):
        """ Prepare netcdf download by parsing through the resulting raw urls """
        import itertools
        download_urls = [rurl['allURLs'][1] for rurl in self.raw()]
        download_list = []
        for durl in download_urls:
            fname = '-'.join(os.path.basename(durl).split('-')[1:])
            nc_urls = get_download_urls(durl)
            download_list.append(
                list(filter(lambda x: (x.count(fname) > 1)
                            and ('cal_' not in x), nc_urls))
            )
        self._last_download_list = list(itertools.chain.from_iterable(download_list))  # noqa
        return self._last_download_list

    def _check_data_status(self, data):
        """ Check if data is ready or not by looking for status.txt"""
        urls = {
            'thredds_url': data['allURLs'][0],
            'status_url': data['allURLs'][1]
        }
        check_complete = '/'.join([urls['status_url'], 'status.txt'])

        req = requests.get(check_complete)
        status_code = req.status_code
        if status_code != 200:
            text = f'Your data ({urls["status_url"]}) is still compiling... Please wait.'
            print(text)  # noqa
            logger.info(text)  # noqa
            return None
        text = f'Request ({urls["status_url"]}) completed.'
        print(text)  # noqa
        logger.info(text)  # noqa
        return urls['thredds_url']

    def _process_request(self):
        """ Sets up thread workers, currently set to 5 """
        for x in range(5):
            thread = threading.Thread(
                name='process-request', target=self._threader)
            # this ensures the thread will die when the main thread dies
            # can set t.daemon to False if you want it to keep running
            thread.daemon = True
            thread.start()

    def _perform_request(self, arg):
        """ Function that perform task from queue """
        # when this exits, the print_lock is released
        with print_lock:
            req = fetch_url(prepped_request=arg, session=self._session)
            if req.json():
                jsonres = req.json()
                if 'status_code' in jsonres:
                    jsonres['request_url'] = req.url
                self._raw_data.append(jsonres)
            logger.debug(arg)

    def _perform_cloud_request(self, arg):
        """ Function that perform task from queue """
        # when this exits, the print_lock is released
        with print_lock:
            selected, start_dt, end_dt = arg
            total_ds = xr.merge([xr.open_zarr(store=s3fs.S3Map(
                sel.uri, s3=FILE_SYSTEM)) for idx, sel in selected.iterrows()])
            self._raw_data.append(selected)
            if len(total_ds.coords) > 0:
                self._dataset_list.append(
                    total_ds.sel(time=slice(start_dt, end_dt)))
            else:
                message = f"{selected.iloc[0].uri} dates {start_dt} to {end_dt} is empty!"
                logger.info(message)
            logger.debug(selected)

    def _threader(self):
        """ Get job from the front of queue and pass to function """
        while True:
            nextq = self._q.get()
            if self._cloud_source:
                self._perform_cloud_request(nextq)
            else:
                self._perform_request(nextq)
            self._q.task_done()

    def _get_instruments_catalog(self):
        """ Get instruments catalog """
        if self._rtoc:
            rtoc = self._rtoc
        else:
            rtoc = fetch_url(requests.Request('GET',
                                              self._OOI_M2M_TOC,
                                              auth=(self.ooi_username,
                                                    self.ooi_token)).prepare(),
                             session=self._session)
            self._rtoc = rtoc

        toc_json = rtoc.json()
        instruments_json = toc_json['instruments']
        return parse_toc_instruments(instruments_json)

    def _get_vocab(self):
        """ Get vocabulary """
        if self._rvocab:
            rvocab = self._rvocab
        else:
            rvocab = fetch_url(requests.Request('GET',
                                                self._OOI_M2M_VOCAB,
                                                auth=(self.ooi_username,
                                                      self.ooi_token)).prepare(),
                               session=self._session)
            self._rvocab = rvocab
        return pd.DataFrame(rvocab.json())

    def _get_rawdata_filelist(self):
        raw_data_url = {
            'CE04OSPS-PC01B': 'https://rawdata.oceanobservatories.org/files/CE04OSPS/PC01B/ZPLSCB102_10.33.10.143',
            'CE02SHBP-MJ01C': 'https://rawdata.oceanobservatories.org/files/CE02SHBP/MJ01C/ZPLSCB101_10.33.13.7'
        }
        raw_datadf = {}
        for ref, raw_url in raw_data_url.items():
            req = requests.get(raw_url)
            if req.status_code == 200:
                page = html_parser(req.content)
            else:
                page = req.status_code

            if not isinstance(page, int):
                files = [(datetime.datetime.strptime(a.get('href'), 'OOI-D%Y%m%d-T%H%M%S.raw'), a.get('href')) for a in page.xpath("//a[re:match(@href, '(\w)+\.raw')]",
                                                                                                                                   namespaces={"re": "http://exslt.org/regular-expressions"})]

                file_df = pd.DataFrame(
                    files, columns=['datetime', 'filename']).set_index('datetime')

                raw_datadf[ref] = file_df

        self._raw_datadf = raw_datadf
        self._raw_data_url = raw_data_url
        return self._raw_datadf, self._raw_data_url

    def _get_data_catalog(self):
        """ Get Data Catalog """
        if self._current_data_catalog:
            return self._current_data_catalog
        else:
            instruments_catalog = self._get_instruments_catalog()
            vocabdf = self._get_vocab()
            streams = self._get_streams()
            parameter_streamdf = self._get_parameter_streams()

            raw_data_catalog = pd.merge(
                instruments_catalog, vocabdf, left_on='reference_designator', right_on='refdes')
            raw_data_catalog = pd.merge(
                raw_data_catalog, streams, left_on='stream', right_on='stream_rd')

            allcat = pd.merge(raw_data_catalog, parameter_streamdf)
            self._current_data_catalog = parse_raw_data_catalog(allcat)
            return self._current_data_catalog

    def _get_global_ranges(self):
        """ Get global ranges in OOI """
        if self._rglobal_range:
            rglobal_range = self._rglobal_range
        else:
            rglobal_range = fetch_url(requests.Request('GET',
                                                       self._OOI_GLOBAL_RANGE).prepare(),
                                      session=self._session)
            self._rglobal_range = rglobal_range
        global_ranges = pd.read_csv(StringIO(rglobal_range.text))

        return parse_global_range_dataframe(global_ranges)

    def _get_streams(self):
        """ Get OOI Streams """
        if self._rstreams:
            rstreams = self._rstreams
        else:
            rstreams = fetch_url(requests.Request('GET',
                                                  self._OOI_M2M_STREAMS,
                                                  auth=(self.ooi_username,
                                                        self.ooi_token)).prepare(),
                                 session=self._session)
            self._rstreams = rstreams
        streamsdf = pd.DataFrame.from_records(rstreams.json()).copy()
        return parse_streams_dataframe(streamsdf)

    def _get_deployments(self, inst):
        """ Get deployment of the inst pandas Series object """
        params = {
            'refdes': inst.reference_designator
        }
        rdeployments = fetch_url(requests.Request('GET',
                                                  self._OOI_M2M_DEPLOYMENT_QUERY,  # noqa
                                                  auth=(self.ooi_username,
                                                        self.ooi_token),
                                                  params=params).prepare(),
                                 session=self._session)
        if rdeployments:
            return parse_deployments_json(rdeployments.json(), inst)
        return None

    def _get_annotations(self, inst):
        """ Get annotations of the inst pandas Series object """
        params = {
            'beginDT': unix_time_millis(parser.parse(inst.begin_date).replace(tzinfo=pytz.UTC)),  # noqa
            'endDT': unix_time_millis(parser.parse(inst.end_date).replace(tzinfo=pytz.UTC)),  # noqa
            'method': inst.stream_method,
            'refdes': inst.reference_designator,
            'stream': inst.stream_rd
        }
        rannotations = fetch_url(requests.Request('GET',
                                                  self._OOI_M2M_ANNOTATIONS,
                                                  auth=(self.ooi_username,
                                                        self.ooi_token),
                                                  params=params).prepare(),
                                 session=self._session)
        if rannotations:
            return parse_annotations_json(rannotations.json())
        return None

    def _get_parameter_streams(self):
        """ Get OOI Parameter Streams """
        if self._rstreams:
            rstreams = self._rstreams
        else:
            rstreams = fetch_url(requests.Request('GET',
                                                  self._OOI_M2M_STREAMS,
                                                  auth=(self.ooi_username,
                                                        self.ooi_token)).prepare(),
                                 session=self._session)
            self._rstreams = rstreams

        streamsdf = pd.DataFrame.from_records(rstreams.json()).copy()
        return parse_parameter_streams_dataframe(streamsdf)

    def _fetch_monthly_stats(self, inst):
        """ Fetched monthly stats for instrument object """
        rmstats = fetch_url(requests.Request('GET', f'{self._OOI_VISUALOCEAN_M_STATS}/{inst.reference_designator}.json').prepare(),  # noqa
                            session=self._session)
        mstatsdf = pd.DataFrame(rmstats.json()).copy()
        mstatsdf.loc[:, 'percentage'] = mstatsdf.percentage.apply(
            lambda row: row * 100.0)
        mstatsdf.loc[:, 'reference_designator'] = inst.reference_designator
        return mstatsdf.rename({
            'stream': 'stream_rd',
            'method': 'stream_method'
        }, axis=1)

    def _use_existing_credentials(self):

        if os.path.exists(CREDENTIALS_FILE):
            import json
            with open(CREDENTIALS_FILE) as f:
                creds = json.load(f)['ooi']
                self.ooi_username = creds['username']
                self.ooi_token = creds['api_key']
        else:
            logger.error('Please authenticate by using yodapy.utils.creds.set_credentials_file')  # noqa
