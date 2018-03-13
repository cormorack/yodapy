# This is a follow-on example script to example1. You
# should have run it at the ipython/jupyter prompt with
#
# %run example1_access_OOI.py

# That script sets us up with an xarray called 'ds' which
# is the dataset we have loaded in. This is basically a
# mirror of the netCDF file that OOI produces.
#
# Now, for some plotting!
# these dependencies should already be there, but uncomment
# if you didn't run example1

# import xarray as xr
# import datetime

import matplotlib as mp
import matplotlib.pyplot as plt
import pandas as pd

# print out the dataset to make sure it is available in our console

try:
    print(ds)
except NameError:
    print("ds not found. Did you run the first example?")
except:
    print("Sorry. There was an error with finding dataset: ds")

# Plot a simple timeseries of temperature, and PSU vs. pressure

# make time the dominant variable for measurements
ds.swap_dims({'obs':'time'})

# pull out values from the xarray structure
pressure = ds['seawater_pressure'].values
temperature = ds['seawater_temperature'].values
psu = ds['practical_salinity'].values
ds_time = ds['time'].values

# quick and dirty timeseries plot of temperature for sanity check
plt.plot(ds_time,temperature)
plt.show()

# Now, more complete colored scatter plot, with data plotted
# on a depth vs. time axis.

fig,(ax1,ax2) = plt.subplots(nrows=2,ncols=1)
fig.set_size_inches(16,9)

# plot temp and salinity curtain plots
ax1.invert_yaxis()
ax1.grid()
ax1.set_xlim(ds_time[0],ds_time[-1])
sc1 = ax1.scatter(ds_time,pressure,c=temperature)
cb = fig.colorbar(sc1,ax=ax1)

ax2.invert_yaxis()
ax2.grid()
ax2.set_xlim(ds_time[0],ds_time[-1])
sc2 = ax2.scatter(ds_time,pressure,c=psu)
cb2 = fig.colorbar(sc2,ax=ax2)

#TODO: Tidy up axis labeling
