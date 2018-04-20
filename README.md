# visualoceanpy
[![Build Status](https://travis-ci.org/cormorack/visualoceanpy.svg?branch=master)](https://travis-ci.org/cormorack/visualoceanpy)

Python API to OOI M2M RESTful Web Services

## Installation

```bash
git clone https://github.com/lsetiawan/visualoceanpy.git
cd visualoceanpy
conda create -n vizocean -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
source activate vizocean
pip install -e .
```

## Credentials
To request data, you will need a json file with your user and token. Example:

```json
{
  "user": "someUSER",
  "token": "TheAwesomeToken"
}
```

## Example running the program

```python
In [1]: from visualocean.core import OOIASSET

In [2]: asset = OOIASSET.from_reference_designator('RS01SBPS-SF01A-2A-CTDPFA102')

In [3]: import datetime

In [4]: stdt = datetime.datetime(2017, 8, 21)

In [5]: enddt = datetime.datetime(2017, 8, 22)

In [6]: asset.request_data(begin_date=stdt, end_date=enddt, credfile='.creds.json')
Please wait while data is compiled.
Out[6]: 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180303T002212-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'

In [7]: asset.thredds_url # Go to url to see the status
Out[7]: 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180303T002212-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'

In [8]: vars(asset)
Out[8]: 
{'_status_url': 'https://opendap.oceanobservatories.org/async_results/landungs@uw.edu/20180303T002212-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample',
 'method': 'streamed',
 'node': 'SF01A',
 'sensor': '2A-CTDPFA102',
 'site': 'RS01SBPS',
 'stream': 'ctdpf_sbe43_sample',
 'thredds_url': 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180303T002212-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'}

In [9]: ds = asset.to_xarray()
Request completed

In [10]: ds
Out[10]: 
<xarray.Dataset>
Dimensions:                     (obs: 86397)
Coordinates:
  * obs                         (obs) int32 0 1 2 3 4 5 6 7 8 9 10 11 12 13 ...
    time                        (obs) datetime64[ns] dask.array<shape=(86397,), chunksize=(86397,)>
    lat                         (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    lon                         (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
Data variables:
    deployment                  (obs) int32 dask.array<shape=(86397,), chunksize=(86397,)>
    id                          (obs) |S64 dask.array<shape=(86397,), chunksize=(86397,)>
    conductivity                (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    driver_timestamp            (obs) datetime64[ns] dask.array<shape=(86397,), chunksize=(86397,)>
    ext_volt0                   (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    ingestion_timestamp         (obs) datetime64[ns] dask.array<shape=(86397,), chunksize=(86397,)>
    internal_timestamp          (obs) datetime64[ns] dask.array<shape=(86397,), chunksize=(86397,)>
    port_timestamp              (obs) datetime64[ns] dask.array<shape=(86397,), chunksize=(86397,)>
    preferred_timestamp         (obs) object dask.array<shape=(86397,), chunksize=(86397,)>
    pressure                    (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    pressure_temp               (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    provenance                  (obs) |S64 dask.array<shape=(86397,), chunksize=(86397,)>
    quality_flag                (obs) |S64 dask.array<shape=(86397,), chunksize=(86397,)>
    temperature                 (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    seawater_temperature        (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    seawater_pressure           (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    seawater_conductivity       (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    practical_salinity          (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    corrected_dissolved_oxygen  (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
    density                     (obs) float64 dask.array<shape=(86397,), chunksize=(86397,)>
Attributes:
    _NCProperties:                      version=1|netcdflibversion=4.4.1.1|hd...
    node:                               SF01A
    comment:                            
    publisher_email:                    
    sourceUrl:                          http://oceanobservatories.org/
    collection_method:                  streamed
    stream:                             ctdpf_sbe43_sample
    featureType:                        point
    creator_email:                      
    publisher_name:                     Ocean Observatories Initiative
    date_modified:                      2018-03-03T00:24:40.307210
    keywords:                           
    cdm_data_type:                      Point
    references:                         More information can be found at http...
    Metadata_Conventions:               Unidata Dataset Discovery v1.0
    date_created:                       2018-03-03T00:24:40.307208
    id:                                 RS01SBPS-SF01A-2A-CTDPFA102-streamed-...
    requestUUID:                        eacbd7b3-3ea3-4b85-95d3-3335641cbfb8
    contributor_role:                   
    summary:                            Dataset Generated by Stream Engine fr...
    keywords_vocabulary:                
    institution:                        Ocean Observatories Initiative
    naming_authority:                   org.oceanobservatories
    feature_Type:                       point
    infoUrl:                            http://oceanobservatories.org/
    license:                            
    contributor_name:                   
    uuid:                               eacbd7b3-3ea3-4b85-95d3-3335641cbfb8
    creator_name:                       Ocean Observatories Initiative
    title:                              Data produced by Stream Engine versio...
    sensor:                             2A-CTDPFA102
    standard_name_vocabulary:           NetCDF Climate and Forecast (CF) Meta...
    acknowledgement:                    
    Conventions:                        CF-1.6
    project:                            Ocean Observatories Initiative
    source:                             RS01SBPS-SF01A-2A-CTDPFA102-streamed-...
    publisher_url:                      http://oceanobservatories.org/
    creator_url:                        http://oceanobservatories.org/
    nodc_template_version:              NODC_NetCDF_TimeSeries_Orthogonal_Tem...
    subsite:                            RS01SBPS
    processing_level:                   L2
    history:                            2018-03-03T00:24:40.307168 generated ...
    Manufacturer:                       Sea-Bird Electronics
    ModelNumber:                        SBE 16plus V2
    SerialNumber:                       16-50115
    Description:                        CTD Profiler: CTDPF Series A
    FirmwareVersion:                    Not specified.
    SoftwareVersion:                    Not specified.
    AssetUniqueID:                      ATAPL-66662-00008
    Notes:                              Not specified.
    Owner:                              University of Washington
    RemoteResources:                    []
    ShelfLifeExpirationDate:            Not specified.
    Mobile:                             False
    AssetManagementRecordLastModified:  2018-02-14T13:26:35.180000
    time_coverage_start:                2017-08-21T00:00:00.803386
    time_coverage_end:                  2017-08-21T23:59:59.015734
    time_coverage_resolution:           P1.00S
    geospatial_lat_min:                 44.52897
    geospatial_lat_max:                 44.52897
    geospatial_lat_units:               degrees_north
    geospatial_lat_resolution:          0.1
    geospatial_lon_min:                 -125.38966
    geospatial_lon_max:                 -125.38966
    geospatial_lon_units:               degrees_east
    geospatial_lon_resolution:          0.1
    geospatial_vertical_units:          meters
    geospatial_vertical_resolution:     0.1
    geospatial_vertical_positive:       down
    DODS.strlen:                        2
    DODS.dimName:                       string2
    DODS_EXTRA.Unlimited_Dimension:     obs


```


