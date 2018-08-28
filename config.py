"""

Configuration file

List directories and sort routine to use

custom_sort_1_elems
  Assuming filename of format  <ssscc_number>
  So sort on first element of filename which is the
  ssscc_number (station as 3 digits and cast as 2 digits)

custom_sort_3_elems
  Assuming filename of format  <alphanumeric>_<station id>_<cast_number>
  So sort on second and third elements 
  which are the station id and cast number

"""

from pathlib import Path


class Config:

  DATA_DIR = Path("/Users/merchant/Work/process_cchdo_data/exchange_ctd/data/")

  RAW_DIR = DATA_DIR.joinpath('raw/')
  OUTPUT_DIR = DATA_DIR.joinpath('output/')
  NETCDF_DIR = OUTPUT_DIR.joinpath('netcdf/')
  MAT_DIR = OUTPUT_DIR.joinpath('mat/')


  SORT_ROUTINE = 'custom_sort_3_elems'
