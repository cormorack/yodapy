from visualocean.core import OOIASSET
import datetime

asset = OOIASSET.from_reference_designator('RS01SBPS-SF01A-2A-CTDPFA102')

# This example is designed to showcase basic data download
# over the time period of the 2017 solar eclipse

stdt = datetime.datetime(2017, 8, 20)
enddt = datetime.datetime(2017, 8, 23)

# The script will need your USER and TOKEN information from
# your OOI account. These are saved in a file called ooi_auth.json
# for this example

asset.request_data(
    begin_date=stdt,
    end_date=enddt,
    credfile='ooi_auth.json'
    )

# you need to wait for the request to complete
# after it completes, you can go to a URL to see whether your
# request is completed.

print(asset.thredds_url) # Go to url to see the status

# now, need to convert from this structure to an pandas xarray
# we'll call it 'ds' for dataset
ds = asset.to_xarray()

# Complete! Now you should be able to pull out various
# streams, and make plots
