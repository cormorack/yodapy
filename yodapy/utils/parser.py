# -*- coding: utf-8 -*-

from __future__ import (division,
                        absolute_import,
                        print_function,
                        unicode_literals)

import datetime

import netCDF4 as nc
import pandas as pd
import numpy as np
from dateutil import parser

from siphon.catalog import TDSCatalog
import pytz

import logging

logger = logging.getLogger(__name__)


def unix_time_millis(dt):
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
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


def build_url(*args):
    return '/'.join(args)


def split_val_list(df, lst_col):
    return pd.DataFrame({
        col: np.repeat(df[col].values,
                       df[lst_col].str.len())
        for col in df.columns.drop(lst_col)}
    ).assign(**{lst_col: np.concatenate(df[lst_col].values)})[df.columns]


def get_value(rowcol):
    if isinstance(rowcol, dict):
        return rowcol['value']


# --- OOI Data Source Specific Parsers ---
def obs_index(ds):
    """Function to create an observations index dataframe by time"""
    indexdf = ds.time.to_pandas().reset_index().set_index(0).resample('d').apply(list)  # noqa
    indexdf.loc[:, 'sel_range'] = indexdf.obs.apply(lambda row: (row[0], row[-1]) if row else pd.NaT)  # noqa
    indexdf.loc[:, 'time'] = indexdf.index
    return indexdf[['time', 'sel_range']].dropna()


def get_nc_urls(thredds_url, download=False, cloud_source=False, **kwargs):
    urltype = 'OPENDAP'
    if download:
        urltype = 'HTTPServer'
    caturl = thredds_url.replace('.html', '.xml')
    cat = TDSCatalog(caturl)
    datasets = cat.datasets
    dataset_urls = []
    if cloud_source:
        strbd = kwargs.get('begin_date')
        stred = kwargs.get('end_date')
        # TODO: Add bd and ed time checking, and warn user if data not available.
        bd = parser.parse(strbd)
        ed = parser.parse(stred)
        dataset_urls = list(map(lambda x: x.access_urls[urltype],
                                datasets.filter_time_range(bd, ed,
                                                           regex=r'(?P<year>\d{4})(?P<month>[01]\d)(?P<day>[0123]\d)')))
        if not dataset_urls:
            logger.warn(
                f'Data not found for specified date range: {strbd} - {stred}')
    else:
        ncfiles = list(filter(lambda d: not any(w in d.name for w in ['json', 'txt', 'ncml']),  # noqa
                        [cat.datasets[i] for i, d in enumerate(datasets)]))  # noqa
        dataset_urls = [d.access_urls[urltype] for d in ncfiles]
    return dataset_urls


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


def get_instrument_list(filtdcat):
    """ Retrieve clean instrument list without any codes """
    return filtdcat[
        ['array_name',
         'site_name',
         'infrastructure_name',
         'instrument_name',
         'stream_method',
         'stream_rd']
    ].sort_values(
        by=['site_name',
            'infrastructure_name',
            'instrument_name']
    ).reset_index(
        drop='index'
    )


def parse_toc_instruments(instruments_json):
    """ Parse instruments table of contents json from M2M """
    raw_instruments = pd.DataFrame(instruments_json)
    instrument_stream = []
    for idx, row in raw_instruments.iterrows():
        for s in row['streams']:
            s['reference_designator'] = row['reference_designator']
            instrument_stream.append(s)
    inst_stream_table = pd.DataFrame(instrument_stream)

    return pd.merge(raw_instruments, inst_stream_table).drop('streams', axis=1)


def parse_raw_data_catalog(raw_data_catalog):
    """ Clean up the raw data catalog dataframe """
    filtered_data_catalog = raw_data_catalog[['tocL1',
                                              'tocL2',
                                              'tocL3',
                                              'reference_designator',
                                              'platform_code',
                                              'mooring_code',
                                              'instrument_code',
                                              'beginTime',
                                              'endTime',
                                              'method',
                                              'stream_content',
                                              'stream_type',
                                              'stream_rd',
                                              'instrument',
                                              'manufacturer',
                                              'model',
                                              'parameter_rd',
                                              'standard_name',
                                              'unit',
                                              'display_name',
                                              'description']]
    filtered_data_catalog.columns = ['array_name', 'site_name', 'infrastructure_name',  # noqa
                                     'reference_designator', 'site_rd', 'infrastructure_rd',  # noqa
                                     'instrument_rd', 'begin_date', 'end_date',
                                     'stream_method', 'stream_content', 'stream_type',  # noqa
                                     'stream_rd', 'instrument_name', 'instrument_manufacturer',  # noqa
                                     'instrument_model', 'parameter_rd', 'standard_name',  # noqa
                                     'unit', 'display_name', 'description']
    final_data_catalog = filtered_data_catalog.copy()

    # Ensure that cabled array sub regions are highlighted
    final_data_catalog.loc[:, 'array_name'] = filtered_data_catalog.array_name.apply(  # noqa
        lambda row: f'{row} (Cabled Array)' if row in ['Cabled Continental Margin',  # noqa
                                                       'Cabled Axial Seamount'] else row)  # noqa
    del filtered_data_catalog
    return final_data_catalog


def parse_streams_dataframe(streamsdf):
    """ Clean up the streams dataframe """
    streamsdf.loc[:, 'stream_type'] = streamsdf.apply(
        lambda row: get_value(row['stream_type']), axis=1)
    streamsdf.loc[:, 'stream_content'] = streamsdf.apply(
        lambda row: get_value(row['stream_content']), axis=1)
    streams = streamsdf[['name', 'stream_content', 'stream_type']]
    streams.columns = ['stream_rd', 'stream_content', 'stream_type']

    return streams


def parse_parameter_streams_dataframe(streamsdf):
    """ Create parameter streams dataframe from the streams dataframe """
    parameter_stream = []
    for idx, row in streamsdf.iterrows():
        for s in row['parameters']:
            s['stream_rd'] = row['name']
            parameter_stream.append(s)
    parameter_streamdf = pd.DataFrame(parameter_stream)[
        ['stream_rd', 'name', 'standard_name',
         'unit', 'display_name', 'description']]
    parameter_streamdf.loc[:, 'unit'] = parameter_streamdf.apply(
        lambda row: get_value(row['unit']), axis=1)

    parameter_streamdf.columns = ['stream_rd', 'parameter_rd',
                                  'standard_name', 'unit', 'display_name',
                                  'description']

    return parameter_streamdf


def parse_global_range_dataframe(global_ranges):
    """ Cleans up the global ranges dataframe """
    global_df = global_ranges[global_ranges.columns[:-3]]
    global_df.columns = ['reference_designator', 'parameter_id_r',
                         'parameter_id_t', 'global_range_min',
                         'global_range_max', 'data_level', 'units']
    return global_df


def parse_deployments_json(dps, inst):
    dep_list = []
    for dep in dps:
        dps_dict = {
            'deployment_id': dep['eventId'],
            'begin_date': pd.to_datetime(int(dep['eventStartTime']), unit='ms').to_pydatetime().replace(tzinfo=pytz.UTC),  # noqa
            'end_date': pd.to_datetime(int(dep['eventStopTime']), unit='ms').to_pydatetime().replace(tzinfo=pytz.UTC) if dep['eventStopTime'] else parser.parse(inst.begin_date).replace(tzinfo=pytz.UTC),  # noqa
            'deployment_number': dep['deploymentNumber'],
            'ref_des': inst.reference_designator,
            'stream_method': inst.stream_method,
            'stream_rd': inst.stream_rd
        }
        if parser.parse(inst.begin_date).replace(tzinfo=pytz.UTC) < dps_dict['begin_date']:
            dps_dict['begin_date'] = parser.parse(
                inst.begin_date).replace(tzinfo=pytz.UTC).isoformat()
        else:
            dps_dict['begin_date'] = dps_dict['begin_date'].isoformat()

        dps_dict['end_date'] = dps_dict['end_date'].isoformat()
        dep_list.append(dps_dict)
    return pd.DataFrame.from_records(dep_list)


def parse_annotations_json(anno_json):
    """ Clean up annotations json """
    annodf = pd.DataFrame(anno_json).copy()
    annodf.loc[:, 'begin_date'] = annodf.beginDT.apply(
        lambda t: pd.to_datetime(t, unit='ms'))
    annodf.loc[:, 'end_date'] = annodf.endDT.apply(
        lambda t: pd.to_datetime(t, unit='ms'))

    # Sort descending by end date
    annodf_sorted = annodf.sort_values(by=['end_date'], ascending=False)
    annodf_sorted = annodf_sorted.drop(['beginDT', 'endDT',  '@class'], axis=1)
    annodf_sorted = annodf_sorted.rename({'exclusionFlag': 'exclusion_flag',
                                          'qcFlag': 'qc_flag',
                                          'sensor': 'instrument_rd',
                                          'stream': 'stream_rd',
                                          'node': 'infrastructure_rd',
                                          'subsite': 'site_rd',
                                          'method': 'stream_method'}, axis='columns')
    annodf_sorted = annodf_sorted[['id',
                                   'site_rd',
                                   'infrastructure_rd',
                                   'instrument_rd',
                                   'annotation',
                                   'begin_date',
                                   'end_date',
                                   'source',
                                   'stream_rd',
                                   'stream_method',
                                   'parameters',
                                   'qc_flag',
                                   'exclusion_flag']]
    return annodf_sorted.reset_index(drop='index')

# --- End OOI Data Source Specific Parsers ---
