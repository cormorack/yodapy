[![Build Status](https://travis-ci.org/cormorack/yodapy.svg?branch=master)](https://travis-ci.org/cormorack/yodapy)
[![Build status](https://ci.appveyor.com/api/projects/status/29rvgs6u8t552ui2?svg=true)](https://ci.appveyor.com/project/lsetiawan/yodapy)
[![Coverage Status](https://coveralls.io/repos/github/cormorack/yodapy/badge.svg?branch=master)](https://coveralls.io/github/cormorack/yodapy?branch=master)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
# yodapy
Your Ocean Data Access in Python

## Installation

```bash
git clone https://github.com/lsetiawan/yodapy.git
cd yodapy
conda create -n yodapy -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
source activate yodapy
pip install -e .
```

## Credentials
To request data, you will need to setup your credential.

```python
>>> from yodapy.utils import set_ooi_credentials_file
>>> set_ooi_credentials_file(username='MyName', token='My secret token')
```

## Example running the program

```python
>>> from yodapy.datasources import OOI
>>> begin_date = datetime.datetime(2018, 1, 1)
>>> end_date = datetime.datetime(2018, 2, 1)
>>> ooi = OOI.search(region=['cabled'], site=['axial base shallow profiler'], instrument=['CTD'], begin_date=begin_date, end_date=end_date)
>>> ooi
Data Source: OOI
        Number of streams: 2
        Streams range: 2014-10-02 to 2018-06-07
>>> dataset_list = ooi.request_data(begin_date, end_date).to_xarray()
```


