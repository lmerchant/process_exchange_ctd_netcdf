# Read in parameters file and use columns determined in filter_body script

# Then drop any columns not these and finally, drop the flag columns for p,s,t,o

# Also process paremeters file to only include columns saved in body file


# Read in first line of filtered body file.  Then will know which columns to keep.

# if parameter column name not in that list, delete it and the value


# save final list to filtered parameter file

import os
import glob
from configparser import ConfigParser, ExtendedInterpolation


CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read('config.ini')


def filter_file():

  split_dir = CONFIG['Paths']['split_dir']

  parameters_file = glob.glob(split_dir + '/parameters.csv')[0]

  parameter_content = get_file_content(parameters_file) 

  modified_content = filter_parameters(parameter_content)  

  filtered_filename = get_filtered_filename(parameters_file) 

  # write modified content to file
  write_content_to_file(modified_content, filtered_filename)    


def get_filtered_filename(datafile):

  data_basename = os.path.basename(datafile)

  filtered_dir = CONFIG['Paths']['filtered_dir']

  filtered_body_filename = os.path.join(filtered_dir, data_basename)

  return filtered_body_filename
  

def filter_parameters(parameter_content):

  # Get first filtered body file to record which variables were kept
  filtered_dir = CONFIG['Paths']['filtered_dir']

  filtered_body_files = glob.glob(filtered_dir + '/*_body.csv')
  filtered_body_filename = filtered_body_files[0]

  body_column_names = get_body_column_names(filtered_body_filename)

  # Only keep parameter columns that are in the filtered body file
  modified_content = get_column_subset(parameter_content, body_column_names) 

  return modified_content


def get_body_column_names(filtered_body_filename):

  with open(filtered_body_filename) as f:
    column_names_line = f.readline().strip()

  content = [name.strip() for name in column_names_line.split(',')]

  return content


def get_file_content(filename):

  # Read in lines of file and remove new line char
  with open(filename) as f:
    file_content = f.read().splitlines()

  return file_content


def get_column_subset(file_content, body_column_names):

  column_names= [name.strip() for name in file_content[0].split(',')]
  data = [value.strip() for value in file_content[1].split(',')]

  # Find subset of desired in existing
  indices_to_delete = [index for index, name in enumerate(column_names) if name not in body_column_names]

  column_names = [x for i,x in enumerate(column_names) if i not in indices_to_delete]
  data = [x for i,x in enumerate(data) if i not in indices_to_delete]

  column_line = ','.join(column_names)
  data_line = ','.join(data)

  content = [column_line, data_line]

  return content


def write_content_to_file(file_content, filename):

  # Write lines from list to file and add new line character
  with open(filename, 'w') as fh:
    for line in file_content:
      fh.write('{0}\n'.format(line))



def main():

  filter_file()



if __name__ == '__main__':
    main()

