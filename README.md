[![Build Status](https://travis-ci.org/cormorack/yodapy.svg?branch=master)](https://travis-ci.org/cormorack/yodapy)
[![Build status](https://ci.appveyor.com/api/projects/status/29rvgs6u8t552ui2?svg=true)](https://ci.appveyor.com/project/lsetiawan/yodapy)
[![Coverage Status](https://coveralls.io/repos/github/cormorack/yodapy/badge.svg?branch=master)](https://coveralls.io/github/cormorack/yodapy?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/cormorack/yodapy/badge/master)](https://www.codefactor.io/repository/github/cormorack/yodapy/overview/master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
# yodapy
Your Ocean Data Access in Python (YODAPY)

## Installation

```bash
pip install yodapy
```

## Development
```bash
git clone https://github.com/cormorack/yodapy.git
cd yodapy
conda create -n yodapy -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
source activate yodapy
pip install -e .
```

## Credentials
To start using yodapy for the ooi datasource, 
you will need to setup your credential file. 
*This will only need be set one time.*

```python
>>> from yodapy.utils.creds import set_credentials_file
>>> set_credentials_file(data_source='ooi', username='MyName', token='My secret token')
```

## Example running the program

```python
>>> from yodapy.datasources import OOI
>>> ooi = OOI()
>>> ooi.search(region='cabled', site='axial base shallow profiler', node='shallow profiler', instrument='CTD')
>>> ooi.view_instruments()
>>> ooi.data_availability()
>>> begin_date = '2018-01-01'
>>> end_date = '2018-01-02'
>>> ooi.request_data(begin_date=begin_date, end_date=end_date)
>>> ooi.check_status()
Request Completed
>>> ds_list = ooi.to_xarray()
```


