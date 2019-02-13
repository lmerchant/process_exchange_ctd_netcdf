"""
Take extracted metadata from ctd csv file 
and process it as follows:

Keep a metadata subset and 
Add decimal date column and value
Shift longitude if negative so that range is 0 to 360 deg.
Set NaN if any values are missing

EXPOCODE
SECT_ID
STNNBR
CASTNO
DATE
TIME
LATITUDE
LONGITUDE
TEMP_SCALE

DEC_YEAR


SECT_ID: If a repeat of a WOCE section, this is the WHP section identifier. 
STNNBR: The originator’s station identifier. 
CASTNO: The originator’s cast number.


TODO:  Sara converted longitude to 360 degree scale but should saved
sets be in -180,180 scale?

TODO: And what about date and time.  Keep as is where date is a number
and time is a 4 character string, or convert to a datetime value that
netcdf would use.  Like days since Jan 1, 1950

"""

import os
import glob
from datetime import datetime
from configparser import ConfigParser, ExtendedInterpolation

from utilities.datetime_to_dec_year import datetime_to_dec_year


CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read('config.ini')


def filter_file():

  # Find subset of metadata to read into Panda Data Frame
  # Add decimal date to metadata

  desired_metadata_names = get_desired_metadata_names()

  split_dir = CONFIG['Paths']['split_dir']

  for metadata_file in glob.glob(split_dir + '/metadata.csv'):

    metadata_content = get_file_content(metadata_file)    

    metadata_names = get_metadata_names(metadata_content, desired_metadata_names)

    modified_content = process_content(metadata_content, metadata_names)  

    # write modified content to file
    filtered_filename = get_filtered_filename(metadata_file)

    write_content_to_file(modified_content, filtered_filename)


def get_file_content(filename):

  # Read in lines of file and remove new line char
  with open(filename) as f:
    file_content = f.read().splitlines()

  return file_content


def get_desired_metadata_names():

  desired_metadata_names = [
    'EXPOCODE',
    'SECT_ID',
    'STNNBR',
    'CASTNO',
    'DATE',
    'TIME',
    'LATITUDE',
    'LONGITUDE',
    'TEMP_SCALE'
  ]

  return desired_metadata_names


def get_metadata_names(metadata_content, desired_metadata_names):

  existing_metadata_names = get_existing_metadata_names(metadata_content)

  # Find subset of desired in existing
  metadata_names = [name for name in existing_metadata_names if name in desired_metadata_names]

  return metadata_names


def get_existing_metadata_names(metadata_content):

  metadata_names_line = metadata_content[0]

  existing_metadata_names = [name.strip() for name in metadata_names_line.split(',')]

  return existing_metadata_names


def process_content(metadata_content, metadata_names):

  # Add decimal date column and value
  # Shift longitude if negative so that range is 0 to 360 deg.
  # Set NaN if any values are missing

  # get subset of columns and values
  metadata_subset = get_metadata_subset(metadata_content, metadata_names) 

  modified_content = modify_longitude(metadata_subset)

  decimal_year = get_decimal_year(metadata_subset)

  modified_content = add_decimal_year(modified_content, decimal_year)

  return modified_content


def get_metadata_subset(metadata_content, subset_metadata_names):

  metadata_names= [name.strip() for name in metadata_content[0].split(',')]
  data = [value.strip() for value in metadata_content[1].split(',')]

  # Find subset of desired in existing
  indices_to_delete = [index for index, name in enumerate(metadata_names) if name not in subset_metadata_names]

  metadata_names = [x for i,x in enumerate(metadata_names) if i not in indices_to_delete]
  data = [x for i,x in enumerate(data) if i not in indices_to_delete]

  column_line = ','.join(metadata_names)
  data_line = ','.join(data)

  modified_content = [column_line, data_line]

  return modified_content


def modify_longitude(metadata_content):

  metadata_names= [name.strip() for name in metadata_content[0].split(',')]
  data = [value.strip() for value in metadata_content[1].split(',')]  

  long_index = metadata_names.index('LONGITUDE')

  longitude = float(data[long_index])

  if longitude < 0:
    longitude = longitude + 360

  data[long_index] = str(longitude)

  column_line = ','.join(metadata_names)
  data_line = ','.join(data)

  modified_content = [column_line, data_line]

  return modified_content


def get_decimal_year(metadata_content):

  date_str, time_str = get_date_and_time(metadata_content)
  datetime_value = get_datetime_from_date_time(date_str, time_str)
  decimal_year = datetime_to_dec_year(datetime_value, 4)

  return decimal_year


def get_date_and_time(metadata_content):

  # Check if data and time values exist.  If they do,
  # return the string values of each
  # TIME is not a required parameter
  date_str = ''
  time_str = ''

  # For CTD, DATE is in format %Y%m%d
  # and TIME is in format %H%M

  # Find column names: 'DATE' and 'TIME' in metadata_names
  # if exist, find values in second line of file
  metadata_names = [name.strip() for name in metadata_content[0].split(',')]
  data = [value.strip() for value in metadata_content[1].split(',')]

  if 'DATE' in metadata_names:
    index = metadata_names.index('DATE')
    date_str = data[index]

  if 'TIME' in metadata_names:
    index = metadata_names.index('TIME')
    time_str = data[index]

  return date_str, time_str


def get_datetime_from_date_time(date_str, time_str):

  date_time_str = date_str + time_str

  if len(time_str) == 0:
    datetime_value = datetime.strptime(date_time_str, '%Y%m%d')  
  else:
    datetime_value = datetime.strptime(date_time_str, '%Y%m%d%H%M')

  return datetime_value


def get_filtered_filename(datafile):

  data_basename = os.path.basename(datafile)

  filtered_dir = CONFIG['Paths']['filtered_dir']

  filtered_body_filename = os.path.join(filtered_dir, data_basename)

  return filtered_body_filename


def add_decimal_year(metadata_content, decimal_year):

  metadata_names= [name.strip() for name in metadata_content[0].split(',')]
  data = [value.strip() for value in metadata_content[1].split(',')]  

  metadata_names.append('DEC_YEAR')
  data.append(str(decimal_year))

  column_line = ','.join(metadata_names)
  data_line = ','.join(data)

  modified_content = [column_line, data_line]

  return modified_content


def write_content_to_file(metadata_content, filename):

  # Write lines from list to file and add new line character
  with open(filename, 'w') as fh:
    for line in metadata_content:
      fh.write('{0}\n'.format(line))



def main():

  filter_file()


if __name__ == '__main__':
    main()


