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

# This particular plot is property-property, looking at
# temperature vs. salinity, a frequently used plot used
# by physical oceanographers to identify different
# water masses.

import xarray as xr
import datetime

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
# NOTE: UNCOMMENT IF YOU HAVEN'T RUN example2_plot_timeseries.py
pressure = ds['seawater_pressure'].values
temperature = ds['seawater_temperature'].values
psu = ds['practical_salinity'].values
ds_time = ds['time'].values

# TODO: calculate T-S density contours to plot underneath

# Now, more complete colored scatter plot, with data plotted
# on a temperature vs. salinty axis, color coded for depth

fig,(ax1) = plt.subplots(nrows=1,ncols=1)
fig.set_size_inches(9,9)

sc1 = ax1.scatter(psu,temperature,c=(-1.0*pressure))
ax1.set_title('Temperature vs. PSU, color coded for Depth')
ax1.set_xlabel('PSU')
ax1.set_ylabel('Temperature (degC)')
cb = fig.colorbar(sc1,ax=ax1)
cb.set_label('Depth (negative decibars)')
plt.show()

# Different way to visualize this data, using hexagonal bins
# and a different colormap to highlight where *most* of the
# T-S properties lie.

fig2,ax2 = plt.subplots(nrows=1,ncols=1)
fig2.set_size_inches(9,9)

hb = ax2.hexbin(psu,temperature,
                gridsize=50,
                bins='log',
                cmap=plt.cm.GnBu)
ax2.set_title('Hexagon Binning')
ax2.set_xlabel('PSU')
ax2.set_ylabel('Temperature (degC)')
cb = fig2.colorbar(hb, ax=ax2)
cb.set_label('log10(N)')
plt.show()

# TODO: Add sigma-t contours to both plots to make mixing
#       across iso-pycnals more obvious
