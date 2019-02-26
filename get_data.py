"""

Get data from files and parse into dataframes

Metadata is from header of file
Parameter is from parameter names and units lines of file
Body is parameter names and data lines of file

input: file list to process
output: parsed into metadata and body dataframes along with lists
of parameter names and units


# TODO
# instead of declaring end line as 1 past it for including in range,
# just add one to end of range and keep ending lines actual line number

"""

import pandas as pd
import datetime as dt


def get_all_data(raw_files):

    metadata_all = []
    body_all = []

    # Then extract metadata and body portion for all files

    is_first_file = True

    for datafile in raw_files:

        # Get all file content into a list
        file_content = get_file_content(datafile)

        # Get and save metadata content to list
        metadata_df, end_metadata_line = get_metadata_content(file_content)
        metadata_all.append(metadata_df)


        # Get parameters from first file since all files will be the same
        if is_first_file:

            parameter_names, parameter_units, end_parameter_line = get_parameter_content(file_content, end_metadata_line) 

            is_first_file = False


        body_df = get_body_content(file_content, parameter_names, end_parameter_line)

        body_all.append(body_df)


        metadata_names = list(metadata_all[0])

    return metadata_all, body_all, metadata_names, parameter_names, parameter_units


def get_file_content(filename):

    # Read in lines of file and remove new line char
    with open(filename) as f:
        file_content = f.read().splitlines()

    return file_content


def get_metadata_content(file_content):

    # Get header line giving number of headers
    header_line, header_line_number = find_header_line(file_content)
    num_headers = get_number_of_headers(header_line)

    # Metadata lines are from header lines
    # end_metadata_line is one past metadata to capture in range
    start_metadata_line = header_line_number + 1
    end_metadata_line = header_line_number + num_headers

    metadata_lines = file_content[start_metadata_line : end_metadata_line]
    metadata_df = extract_metadata(metadata_lines)

    return metadata_df, end_metadata_line


def find_header_line(file_content):

    # Looking for header line of form: NUMBER_HEADERS = 10
    # This indicates the total number of metadata header lines
    # starting at the line below this one.

    line_number = 0

    for line in file_content:
        if ('NUMBER_HEADERS' in line) & (line[0] != '#'):
            header_line = line
            break
        else:
            line_number += 1  

    return header_line, line_number


def get_number_of_headers(header_line):

    # header line is of form: NUMBER_HEADERS = 10
    num_headers = header_line.split('=')[1].strip()
    
    return int(num_headers)


def extract_metadata(metadata_lines):

    # Create a dict of metadata name, value pairs
    # and then convert to dataframe
    metadata_content = {}

    for metadata_line in metadata_lines:
 
        metadata = metadata_line.split('=')

        metadata_name = metadata[0].strip()
        metadata_value = metadata[1].strip()

        metadata_content[metadata_name] = metadata_value

    metadata_df = pd.DataFrame(metadata_content, index=[0])




    #Add datetime column from date and time columns
    #If datetime can't be created, the value created = NaT
    # since datetime is np.datetime64, this is a time delta
    # and so the datetime value is a difference from the min
    # datetime. This is called a proleptic_gregorian

    if 'TIME' in metadata_df.columns:
        datetime_str = metadata_df['DATE'] + metadata_df['TIME']
        metadata_df['DATETIME'] =  pd.to_datetime(datetime_str, format='%Y%m%d%H%M', errors='coerce')
    else:
        datetime_str = metadata_df['DATE']
        metadata_df['DATETIME'] =  pd.to_datetime(datetime_str, format='%Y%m%d', errors='coerce')

    # # Convert datetime into seconds from 1/1/1970
    # metadata_df['SECS_FROM_1970'] = (metadata_df['DATETIME'] - dt.datetime(1970,1,1)).dt.total_seconds()

    # rename dataframe index (column name representing rows)
    #metadata_df.index.names = ['Meta_index']


    # if 'TIME' in metadata_df.columns:
    #   metadata_df['DATETIME'] = metadata_df['DATE'] + metadata_df['TIME']
    # else:
    #   metadata_df['DATETIME'] = metadata_df['DATE']


    return metadata_df  


def get_parameter_content(file_content, end_metadata_line):

    parameter_units = {}

    # Parameter lines are following header lines and
    # consist of the parameter names and corresponding units
    # end_metadata_line is one past metadata to capture in 
    # metadata range
    # end_parameter_line is one past parameter line to 
    # capture in range
    start_parameter_line = end_metadata_line
    end_parameter_line = end_metadata_line + 2
    
    parameter_lines = file_content[start_parameter_line : end_parameter_line]

    parameter_names = [x.strip() for x in parameter_lines[0].split(',')]
    units = [x.strip() for x in parameter_lines[1].split(',')]

    for index, name in enumerate(parameter_names):
        parameter_units[name] = units[index]


    return parameter_names, parameter_units, end_parameter_line


def get_body_content(file_content, parameter_names, end_parameter_line):

    # Body lines will include the parameter name line and then all
    # following data lines except the line containg 'END_DATA'
    end_body_line = find_end_body(file_content)

    main_body = file_content[end_parameter_line : end_body_line]

    body_df = pd.DataFrame([line.split(',') for line in main_body])


    # Add column names
    body_df.columns = parameter_names

    # rename dataframe index (column name representing rows)
    body_df.index.names = ['N_level']
 
    return body_df


def find_end_body(file_content):

    # Find line starting with END_DATA
    # Start looking from the end of the file content list
    for line_number in reversed(range(len(file_content))):
        if 'END_DATA' in file_content[line_number]:
            break  

    return line_number
