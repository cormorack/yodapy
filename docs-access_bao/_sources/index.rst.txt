.. yodapy documentation master file, created by
   sphinx-quickstart on Tue Jun 19 09:11:35 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to yodapy's documentation!
==================================

Yodapy (Your Ocean Data Access in Python) is a tool for accessing ocean related datasets. 
Currently, only Ocean oceanobservatories Initiative (OOI) data is available for access.
This python library was developed by the Cabled Array Value Add Team at the University of Washington.

**Installation**::

    pip install yodapy


**Install directly from github**

For developers and testers:
::
    
    pip install git+https://github.com/cormorack/yodapy.git


**Development**::

    git clone https://github.com/cormorack/yodapy.git
    cd yodapy
    conda create -n yodapy -c conda-forge --yes python=3.6 --file requirements.txt --file requirements-dev.txt
    source activate yodapy
    pip install -e .


**Credentials**

To obtain credentials you are obliged to *register* at the [OOI data portal](https://ooinet.oceanobservatories.org/).
Select the **Log In** dropdown and click **Register**. Fill out and submit the form and you will automatically
be logged in. Click on your email ID (upper right) to visit/edit your profile. This profile now includes
your credentials. You should click on the button **Refresh API Token** to get a stable token; and then make a note
of both your username (format **OOIAPI-XXXXXXXXXXXXXX**) and your token (format **XXXXXXXXXXX**). They are
used in what follows.

To start using yodapy for the ooi datasource, 
you will need to setup your credential file. 
*This will only need be set one time.*::

    >>> from yodapy.utils.creds import set_credentials_file
    >>> set_credentials_file(data_source='ooi', username='MyName', token='My secret token')


**Example running the program**::

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

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   modules


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
