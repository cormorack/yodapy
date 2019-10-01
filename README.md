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

## Install directly from github

For developers and testers:

```bash
pip install git+https://github.com/cormorack/yodapy.git
```


## Implication of installing `yodapy` in a Jupyter notebook environment


In a Jupyter environment the operating system is a static image; which has a certain set of packages installed.
Let's assume this does not include `yodapy` or that it includes an older version of `yodapy` and I am
interested in a more recent version. 
If I install a new (say `yodapy`) package during a notebook session: That does not register in the stored static image. The 
only thing that persists in my computing environment is my home directory. This includes code, repos, data, 
ancillary files and so on; but it does not include Python packages that I install *on the fly*.


This suggests a question: Can I put some installation commands in my `.cshrc` file so that it runs automatically
whenever I start up a new image?



## Development
```bash
git clone https://github.com/cormorack/yodapy.git
cd yodapy
conda create -n yodapy -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
source activate yodapy
pip install -e .
```

## Credentials
To obtain credentials you are obliged to *register* at the [OOI data portal](https://ooinet.oceanobservatories.org/).
Select the **Log In** dropdown and click **Register**. Fill out and submit the form and you will automatically
be logged in. Click on your email ID (upper right) to visit/edit your profile. This profile now includes
your credentials. You should click on the button **Refresh API Token** to get a stable token; and then make a note
of both your username (format **OOIAPI-XXXXXXXXXXXXXX**) and your token (format **XXXXXXXXXXX**). They are
used in what follows.

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


