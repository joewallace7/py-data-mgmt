# Python Data Management Tools

**File:** datamgmt.py

## Purpose

This is a single Python script with a collection of functions meant to make data management, particularly
extracting and importing data to/from databases easier. You'll need to have ODBC connections
already set up to use the file_to_db() and querydb() functions.

Other capabilities include:

* Reading and writing delimited text files (CSV, TSV, pipe-delimted, etc).
* Finding files by part of the name.
* Reading Excel spreadsheets.
* Cleaning strings of non-standard characters.
* Generating UUIDs.
* Getting file line counts.
* Getting the top N lines of a file.


## Dependencies

* pypyodbc: for ODBC connections (https://pypi.org/project/pypyodbc/)
* xlrd: Reading Excel files (https://pypi.org/project/xlrd/)

## Functions

Here's some of the useful functions you'll find in datamgmt.py:

* **file_to_db(...)**:  Streams the file directly to the DB without reading into memory. Useful for when you need to send very large files to a DB.
* **querydb(...)**: Execute SQL query and return results to a file instead of RAM. Exports results to results.txt unless specified otherwise. Useful when result dataset would be very large or you want to directly store to disk without any further processing.
* **string_clean(x)**: Removes all miscellaneous characters from "x" and replaces them with spaces. Keeps most "normal" characters.
* **find_files(x)**: Finds files that have a name containing "x" and returns a list with the file names.
* **int2(x)**: Tries to convert "x" to an integer. If it can't it just returns the original value.
* **div2(n,d)**: Try to divide n/d. Accounts for possible errors, such as division by zero. If failure occurs, returns None.
* **get_uuid()**: Returns a unique identifier
* **hash_value(x)**: Returns the SHA256 hash of "x"
* **get_file_line_count(x)**: Returns the number of lines in file "x"
* **file_header(x,n=10)**: Returns the top n lines from file "x"
