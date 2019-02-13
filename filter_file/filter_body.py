"""
Take extracted body from ctd csv file 
and process it as follows:

Read into a Pandas DataFrame to analyze.

Only interested in following columns:
Pressure and flag, Temperature and flag,
Salinity and flag, Oxygen and flag,

And if exists: Transmissometer and Fluorometer

TODO: Add conditions if more than one Transmissometer column


pressure: CTDPRS (DBAR)
temperature: CTDTMP (ITS-90)
salinity: CTDSAL (PSS-78)
oxygen: CTDOXY (UMOL/KG)

Transmissometer units?
Flurometer units?



Exchange CTD Parameters and Flags

URL of Exchange Common Parameters
http://exchange-format.readthedocs.io/en/latest/parameters.html#common-parameters

URL of WOCE CTD Quality Codes represented in the flag of 
each parameter above.
http://exchange-format.readthedocs.io/en/latest/quality.html#woce-ctd-quality-codes

<PARAMETER_NAME>_FLAG_W

1 Not calibrated.
2 Acceptable measurement.
3 Questionable measurement.
4 Bad measurement.
5 Not reported.
6 Interpolated over a pressure interval larger than 2 dbar.
7 Despiked.
(8) Not used for CTD data.
9 Not sampled.


"""

import os
import glob
import itertools
import numpy as np
import pandas as pd
from configparser import ConfigParser, ExtendedInterpolation


CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read('config.ini')


def process_folder():

  # loop through files in body folder that start
  # with sample name

  # To process data file, also need to know Temperature scale in case
  # the temperatures need to be transformed to another scale
  # Conver to ITS-90 scale if it is in IPTS-68 scale.
  # Read in corresponding parameters file

  # Find subset of columns to read into Panda Data Frame

  split_dir = CONFIG['Paths']['split_dir']

  parameters_file = glob.glob(split_dir + '/parameters.csv')[0]

  temperature_scale = get_temperature_scale(parameters_file)

  desired_column_names = get_desired_column_names()


  for filename in os.listdir(split_dir):

    if 'body' in filename:

      datafile = os.path.join(split_dir, filename)

      column_names = get_column_names(parameters_file, desired_column_names)

      process_file(datafile, temperature_scale, column_names)


def get_desired_column_names():

  main_names = [
    'CTDPRS',
    'CTDTMP',
    'CTDSAL',
    'CTDOXY'
  ]

  transmissometer_names = [
    'TRANSM',
    'TRANSC',
    'XMISS',
    'CTDXMISS',
    'CTDBEAMCP'
  ]

  fluorometer_names = [
    'FLUOR',
    'FLOUR',
    'FLUORM',
    'FLUORC',
    'CTDFLUOR'
  ]

  all_data_names = list(itertools.chain(main_names, transmissometer_names, fluorometer_names))

  # For found column names, look for corresponding flag columns
  #  column name appended with _FLAG_W
  all_flag_names = [s + '_FLAG_W' for s in all_data_names]

  desired_column_names = list(itertools.chain(all_data_names, all_flag_names))

  return desired_column_names


def get_temperature_scale(parameters_file):

  # Read temperature scale from parameter file

  # Read in parameters_file
  content = get_file_content(parameters_file)

  parameters_line = content[0]
  units_line = content[1]

  # Split parameters_line and units_line by comma
  # From the parameters_line, find index of CTDTMP
  # Then get temperature scale at this index
  # Scale is of type ITS-90 or IPTS-68 

  parameters_list = parameters_line.split(',')
  units_list = units_line.split(',')

  temperature_index = [index for index, item in enumerate(parameters_list) if item.strip() == 'CTDTMP']

  temperature_scale = units_list[temperature_index[0]]  

  return temperature_scale.strip()


def get_file_content(filename):

  # Read in lines of file and remove new line char
  with open(filename) as f:
    file_content = f.read().splitlines()

  return file_content


def get_column_names(filename, desired_column_names):

  existing_column_names = get_existing_column_names(filename)

  # Find subset of desired in existing
  column_names = [name for name in existing_column_names if name in desired_column_names]

  return column_names


def get_existing_column_names(filename):

  # Read in file and get first line
  content = get_file_content(filename)

  column_names_line = content[0]

  existing_column_names = [name.strip() for name in column_names_line.split(',')]

  return existing_column_names


def process_file(datafile, temperature_scale, column_names):

  # Read in data file into a Panda DataFrame and process it
  # Want to filter out data columns and then 
  # filter out data with bad flags or mark with NaN

  ctd_pd = pd.read_csv(datafile, usecols=column_names)

  # If any values written as a string (like 'nan'), convert to NaN
  ctd_pd = ctd_pd.applymap(lambda x: np.nan if isinstance(x, str) else x)


  # Transform temperature if needed
  if temperature_scale == 'IPTS-68':
    ctd_pd['CTDTMP'] = ctd_pd['CTDTMP'].apply(t90_to_t68)

  # If a row has both Temperature and Salinity equal to 0, set 
  # pressure, temperature, salinity, and oxgen values to NaN
  ctd_pd.loc[(ctd_pd['CTDTMP'] == 0) & (ctd_pd['CTDSAL'] == 0), ['CTDPRS', 'CTDTMP', 'CTDSAL', 'CTDOXY']] = np.nan

  # If any pressure, temperature, salinity, or oxygen cells have a 
  # value <= -99, set the cell to NaN
  ctd_pd.loc[ctd_pd['CTDPRS'] <= -99, ['CTDPRS']] = np.nan
  ctd_pd.loc[ctd_pd['CTDTMP'] <= -99, ['CTDTMP']] = np.nan
  ctd_pd.loc[ctd_pd['CTDSAL'] <= -99, ['CTDSAL']] = np.nan
  ctd_pd.loc[ctd_pd['CTDOXY'] <= -99, ['CTDOXY']] = np.nan

  ctd_pd.loc[ctd_pd['CTDXMISS'] <= -99, ['CTDXMISS']] = np.nan
  ctd_pd.loc[ctd_pd['CTDFLUOR'] <= -99, ['CTDFLUOR']] = np.nan

  # If any pressure, temperature, salinity, or oxygen flags
  # are not equal to 2, 6, or 7, set corresponding value to NaN
  bad_flags = [1, 3, 4, 5, 8, 9]

  ctd_pd.loc[ctd_pd['CTDPRS_FLAG_W'].isin(bad_flags), ['CTDPRS']] = np.nan
  ctd_pd.loc[ctd_pd['CTDTMP_FLAG_W'].isin(bad_flags), ['CTDTMP']] = np.nan
  ctd_pd.loc[ctd_pd['CTDSAL_FLAG_W'].isin(bad_flags), ['CTDSAL']] = np.nan
  ctd_pd.loc[ctd_pd['CTDOXY_FLAG_W'].isin(bad_flags), ['CTDOXY']] = np.nan


  bad_flags = [9]
  ctd_pd.loc[ctd_pd['CTDXMISS_FLAG_W'].isin(bad_flags), ['CTDXMISS']] = np.nan
  ctd_pd.loc[ctd_pd['CTDFLUOR_FLAG_W'].isin(bad_flags), ['CTDFLUOR']] = np.nan



  # Remove flag columns for CTDPRS, CTDTMP, CTDSAL, CTDOXY before saving
  # Keep flag columns for Transmissometer and Fluorometer
  flag_columns = ['CTDPRS_FLAG_W', 'CTDTMP_FLAG_W', 'CTDSAL_FLAG_W', 'CTDOXY_FLAG_W']

  ctd_pd.drop(columns=flag_columns, inplace=True)


  # Save results to filtered_body file
  filtered_filename = get_filtered_filename(datafile)

  # Save without index column and save NaN as string
  ctd_pd.to_csv(filtered_filename, na_rep='NaN', index=False)


def t90_to_t68(x):
  return 1.00024 * x


def get_filtered_filename(datafile):

  data_basename = os.path.basename(datafile)

  filtered_dir = CONFIG['Paths']['filtered_dir']

  filtered_body_filename = os.path.join(filtered_dir, data_basename)

  return filtered_body_filename


def main():

  process_folder()


if __name__ == '__main__':
    main()


