"""

Get a sorted list of raw files to convet

"""

from pathlib import Path


def get_sorted_files(source_dir, sort_routine):

  file_list = [file for file in source_dir.glob('*.csv')]

  sorted_file_list = sort_files(file_list, sort_routine)

  return sorted_file_list


def sort_files(file_list, sort_routine):

  sorted_list = sorted(file_list, key = exec(sort_routine))

  return sorted_list


def custom_sort_1_elems(filename):
  # Assuming filename of format  <ssscc_number>
  # So sort on first element of filename which is the
  # ssscc_number (station as 3 digits and cast as 2 digits)

  path_parts = list(filename.parts)
  file_parts = path_parts[-1].split('_')

  return file_parts[0]


def custom_sort_3_elems(filename):
  # Assuming filename of format  <alphanumeric>_<station id>_<cast_number>
  # So sort on second and third elements 
  # which are the station id and cast number

  path_parts = list(filename.parts)
  file_parts = path_parts[-1].split('_')


  return file_parts[1] + file_parts[2]

