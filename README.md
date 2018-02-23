# visualoceanpy
[![Build Status](https://travis-ci.org/lsetiawan/visualoceanpy.svg?branch=master)](https://travis-ci.org/lsetiawan/visualoceanpy)

Python API to OOI M2M RESTful Web Services

## Installation

```bash
git clone https://github.com/lsetiawan/visualoceanpy.git
cd visualoceanpy
conda create -n vizocean -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
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
Please wait while data is compiled.https://opendap.oceanobservatories.org/async_results/landungs@uw.edu/20180223T163413-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample
Request completed
Out[6]: 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180223T163413-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'

In [7]: vars(asset)
Out[7]:
{'method': 'streamed',
 'node': 'SF01A',
 'sensor': '2A-CTDPFA102',
 'site': 'RS01SBPS',
 'stream': 'ctdpf_sbe43_sample',
 'thredds_url': 'https://opendap.oceanobservatories.org/thredds/catalog/ooi/landungs@uw.edu/20180223T163413-RS01SBPS-SF01A-2A-CTDPFA102-streamed-ctdpf_sbe43_sample/catalog.html'}

In [8]: ds = asset.to_xarray()

```


