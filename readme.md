# Python Data Management Tools

Use the file datamgmt.py

## Purpose
This is a single Python script with a collection of functions meant to make data management, particularly
extracting and importing data to/from databases easier.

## Dependencies

* pypyodbc: for ODBC connections
* xlrd: Reading Excel files

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
