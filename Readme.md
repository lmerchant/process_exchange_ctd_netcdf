# Convert exchange ctd to xarray NetCDF

### Program overview

Read in folder of csv ctd files and for each file, extract out data portion (body) to a dataframe and the header information (metadata) to a dataframe. Since the parameter names and units are the same for all files, extract info from one file to a list. The parameter names are used as column names in each body dataframe.

Each body dataframe is appended into a list and each metadata dataframe is appended into a list.  The body and metadata dataframes are added to an xarray dataset as variables and the parameter units are added as attributes for these variables. The files were sorted on the filename and so when appended into a list, they are in Profile order. Assuming filename of format <alphanumeric>_<station id>_<cast_number>

Each body and metadata dataframe lists are added to their own xarray dataset and the two xarry datasets are then merged into one. The order of dimensions is (N_profile, N_level) for the body and (N_profile, Metadata_index) for the metadata. The xarray dimension N_profile keeps track of each dataframe. The xarray dimension N_level represents each row of data in the csv file, and Metadata_index is there because I didn't finish looking into multi-dimension vs scalar when creating merged metadata dataframes and creating the metadata xarray.

Attributes for each variable are added to the xarray dataset along with global attributes.  Then the dataset is saved as NetCDF.

### Program details

#### Assigning data types

Before each dataframe is added to an xarray dataset, the data types are set to override any defaults when text was imported to the dataframe.  The metadata data types are set to object except for lat and lon which are set to float64 and for any datetime or timedelta types.  The parameter data types are set to float64 except flag columns which are set to int8 and NaN fill values replaced with 9.  


#### Assigning attributes

For metadata attributes, the long name and units attributes are set via if statements.  In future, read in values using a spreadsheet.  

For parameter attributes, the units attributes are set and if a flag variable, the fill value attribute is set.  


python datetime, np.datetime64, xarray converts 
Put in days since 1970 for netcdf output

xarray will convert to right type

Need to check proper format of any datetime or timedelta values for NetCDF output


