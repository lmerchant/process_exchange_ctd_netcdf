"""
Process exchange ctd files into netcdf files
"""

"""

Read in all files and then separate out into body section and 
metadata section from the header.  The parameters and units will
be the same for all.

For netcdf, want to save all information as a variable and use N-levels
as the index and N_profiles to keep track of each ctd set.


xarray uses dims and coords to enable its core metadata aware operations. 
Dimensions provide names that xarray uses instead of the axis argument found in 
many numpy functions. Coordinates enable fast label based indexing and alignment, 
building on the functionality of the index found on a pandas DataFrame or Series.


Definition of xarray dataset from http://xarray.pydata.org/en/stable/data-structures.html#dataset

xarray.Dataset is xarray’s multi-dimensional equivalent of a DataFrame. 

It is a dict-like container of labeled arrays (DataArray objects) with aligned 
dimensions. It is designed as an in-memory representation of the data model from 
the netCDF file format.

In addition to the dict-like interface of the dataset itself, which can be used 
to access any variable in a dataset, datasets have four key properties:

dims: a dictionary mapping from dimension names to the fixed length of each 
dimension (e.g., {'x': 6, 'y': 6, 'time': 8})

data_vars: a dict-like container of DataArrays corresponding to variables

coords: another dict-like container of DataArrays intended to label points used 
in data_vars (e.g., arrays of numbers, datetime objects or strings)

attrs: an OrderedDict to hold arbitrary metadata

The distinction between whether a variables falls in data or coordinates (
borrowed from CF conventions) is mostly semantic, and you can probably get away 
with ignoring it if you like: dictionary like access on a dataset will supply 
variables found in either category. However, xarray does make use of the 
distinction for indexing and computations. Coordinates indicate constant/fixed/
independent quantities, unlike the varying/measured/dependent quantities that 
belong in data.

In this program, do not want N_level or N_profile set as xarray coordinates.

"""

from pathlib import Path
import numpy as np
import scipy.io as sio
import pandas as pd
import xarray as xr

from config import Config

from get_files import get_sorted_files
from get_data import get_all_data


# Read in all files in the raw folder, sort, and then 
# accumulate the body section into a list and the 
# metadata into a list.  From first body file, extract
# the parameter names and units.

# Metadata is from header of file
# Parameter is from parameter names and units lines of file
# Body is parameter names and data lines of file


def create_folders():

  # Create raw directory to contain exchange ctd csv files
  # User needs to copy files to process into this directory
  Config.RAW_DIR.mkdir(parents=True, exist_ok=True)

  # Create output directory for netcdf file
  Config.NETCDF_DIR.mkdir(parents=True, exist_ok=True)


def process_folder(raw_dir):

  print('Get data')
  # Get sorted list of files in exchange ctd format to convert
  raw_files = get_sorted_files(raw_dir, Config.SORT_ROUTINE)

  # Get data from files and parse into dataframes and lists
  metadata_all, body_all, metadata_names, parameter_names, parameter_units = get_all_data(raw_files)


  # Get metadata and parameter data types
  metadata_dtypes = get_metadata_dtypes(metadata_names)
  parameter_dtypes = get_parameter_dtypes(parameter_units)


  # Add all data and attributes to an xarray
  print('Create xarrays')

  # Fill values if NaN
  fill_value = {'flag': 9, 'datetime': np.datetime64('NaT')}

  # Create xarray to hold body data
  ctd_xr = add_body_to_xarray_dataset(body_all, parameter_names, parameter_dtypes, fill_value)

  # Create xarray to hold metadata
  metadata_xr = add_metadata_to_xarray_dataset(metadata_all, metadata_names, metadata_dtypes, fill_value)

  # Merge body and metadata xarrays into one
  ctd_xr = merge_in_metadata_dataset(metadata_names, metadata_xr, ctd_xr)

  # Add NetCDF attributes to xarray
  ctd_xr = add_metadata_attributes_to_xarray(metadata_names, ctd_xr)
  ctd_xr = add_parameter_attributes_to_xarray(parameter_units, ctd_xr, fill_value)
  ctd_xr = add_global_attributes_to_xarray(ctd_xr)

  print(ctd_xr)

  print(ctd_xr['DAYS_FROM_1970'])


  print('Save as NetCDF')
  # Convert xarray to NetCDF format and save
  save_as_netcdf(ctd_xr)


  print('Save as Mat')
  # Convert NetCDF format to mat format and save
  #save_as_mat(ctd_xr)


def get_metadata_dtypes(metadata_names):
  
  metadata_dtypes = {}

  # iterate through metadata and set dtype
  # dtype is object (string) by default
  for name in metadata_names:
 
    metadata_dtypes[name] = object

    if name == 'LATITUDE':
      metadata_dtypes[name] = np.float64

    if name == 'LONGITUDE':
      metadata_dtypes[name] = np.float64

    if name == 'DEPTH':
      metadata_dtypes[name] = np.float64

    if name == 'DATETIME':
      metadata_dtypes[name] = np.datetime64

    if name == 'DAYS_FROM_1970':
      metadata_dtypes[name] = np.timedelta64      


  return metadata_dtypes


def get_parameter_dtypes(parameter_units):
  
  parameter_dtypes = {}

  # iterate through parameters and set dtype
  for key, value in parameter_units.items():

    if 'FLAG' in key:
      parameter_dtypes[key] = np.int8

    else:
      parameter_dtypes[key] = np.float64


  return parameter_dtypes


def add_body_to_xarray_dataset(body_all, parameter_names, parameter_dtypes, fill_value):

  # index column of each body dataframe was renamed 'N_level'
  # The xarray dimension N_profile keeps track of each dataframe

  # Create xarray dataset from list of dataframes with
  # Dimension order (N_level, N_profile)
  ctd_xr = xr.concat([df.to_xarray() for df in body_all], dim = 'N_profile')


  # Convert to dataframe and assign data types
  ctd_pd = ctd_xr.to_dataframe()

  # Fill NaN values in qc flags with an interger fill value
  for name in parameter_names:
    if 'FLAG' in name and parameter_dtypes[name] == np.int8:
      ctd_pd.loc[ctd_pd[name].isnull(), [name]] = fill_value['flag']

  # Assign data types
  ctd_pd = ctd_pd.astype(parameter_dtypes)

  # Convert back to xarray
  ctd_xr = ctd_pd.to_xarray()

  # Transpose dimension order to (N_profile, N_level)
  ctd_xr = ctd_xr.transpose()

  return ctd_xr


def add_metadata_to_xarray_dataset(metadata_all, metadata_names, metadata_dtypes, fill_value):

  # index column of each body dataframe was renamed 'N_level'
  # The xarray dimension N_profile keeps track of each dataframe

  # Create xarray dataset from list of dataframes with
  # Dimensions: (N_level, N_profile)
  metadata_xr = xr.concat([df.to_xarray() for df in metadata_all], dim = 'N_profile')


  # Convert to dataframe and assign data types
  metadata_pd = metadata_xr.to_dataframe()

  print(metadata_pd['DAYS_FROM_1970'])

  # TODO  Do this after merge metadata with ctd


  # Fill NaN values in datetime with NaT
  for name in metadata_names:
    if 'DATETIME' or 'DAYS_FROM_1970' in name:
      metadata_pd.loc[metadata_pd[name].isnull(), [name]] = fill_value['datetime']

  # Assign data types
  metadata_pd = metadata_pd.astype(metadata_dtypes)

  # Convert back to xarray
  metadata_xr = metadata_pd.to_xarray()

  # Transpose dimension order to (N_profile, N_level)
  metadata_xr = metadata_xr.transpose()

  return metadata_xr


def merge_in_metadata_dataset(metadata_names, metadata_xr, ctd_xr):

  ctd_xr = xr.merge([metadata_xr, ctd_xr])

  # Drop N_level and N_profile as coordinates from dataset
  ctd_xr.drop(['N_profile', 'N_level'])

  return ctd_xr


def add_metadata_attributes_to_xarray(metadata_names, ctd_xr):

  # Add netcdf attributes
  for name in metadata_names:

    if name == 'LATITUDE':
      ctd_xr['LATITUDE'].attrs = {'units':'degrees_North', 'long_name':'Latitude'}

    if name == 'LONGITUDE':
      ctd_xr['LONGITUDE'].attrs = {'units':'degrees', 'long_name':'Longitude'}

    # TODO: Find out what units to use for date, time, dec_year, stnnbr, castno

    if name == 'DATE':
      ctd_xr['DATE'].attrs = {'units':'yyyymmdd', 'long_name':'Date'}

    if name == 'TIME':
      ctd_xr['TIME'].attrs = {'units':'hhmm', 'long_name':'Time'}

    if name == 'DEC_YEAR':
      ctd_xr['DEC_YEAR'].attrs = {'units':'yyyy.####', 'long_name':'Decimal Year'}

    if name == 'STNNBR':
      ctd_xr['STNNBR'].attrs = {'units':'number', 'long_name':'Station Number'}

    if name == 'CASTNO':
      ctd_xr['CASTNO'].attrs = {'units':'number', 'long_name':'Cast Number'}

    if name == 'DEPTH':
      ctd_xr['DEPTH'].attrs = {'units':'meters', 'long_name':'Depth'}      

  return ctd_xr


def add_parameter_attributes_to_xarray(parameter_units, ctd_xr, fill_value):

  for name in parameter_units:

    if 'FLAG' in name:
      ctd_xr[name].attrs = {'_FillValue': fill_value['flag']} 
    else:
      ctd_xr[name].attrs = {'units': parameter_units[name]} 

  return ctd_xr


def add_global_attributes_to_xarray(ctd_xr):

  ctd_xr.attrs['title'] = 'CTD data'

  return ctd_xr


def save_as_netcdf(ctd_xr):

  # Save xarray as netcdf

  # set encoding
  #encoding_attrs = {'DAYS_FROM_1970':{'units':'days since 1970-01-01'}}

  # Get expocode to include in filename
  expocode = ctd_xr['EXPOCODE'][0].values[0]

  filename = expocode + '_dt.nc'

  netcdf_filename = Config.NETCDF_DIR.joinpath(filename)

  try:
    os.remove(netcdf_filename)
  except:
    pass

  #ctd_xr.to_netcdf(netcdf_filename, encoding=encoding_attrs)
  ctd_xr.to_netcdf(netcdf_filename)


def save_as_mat(ctd_xr):

  # Get expocode to include in filename
  expocode = ctd_xr['EXPOCODE'][0].values[0]

  filename = expocode + '.mat'

  mat_filename = Config.MAT_DIR.joinpath(filename)

  try:
    os.remove(mat_filename)
  except:
    pass


  ctd_dict = ctd_xr.to_dict()

  #print(ctd_dict['data_vars']['CTDFLUOR'])

  # {'data': [[values]], 'dims': ('N_profile', 'N_level'), 'attrs': {'units': 'MG/M^3'}}

  ctd_fluor = ctd_dict['data_vars']['CTDFLUOR']

  ctd_fluor_data = ctd_fluor['data']

  N_profile = len(ctd_fluor_data)
  N_level = len(ctd_fluor_data[0])

  #print(N_profile, N_level)

  ctd_fluor_units = ctd_fluor['attrs']['units']

  #print(ctd_fluor_units)



  sio.savemat( mat_filename, dict( [ ('CTDFLUOR', ctd_fluor_data), ('units', ctd_fluor_units) ] ) ) 






def main():

  create_folders()

  process_folder(Config.RAW_DIR)

  



if __name__ == '__main__':
  main()
