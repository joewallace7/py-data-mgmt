# Python Data Management Tools

**File:** datamgmt.py

**This software is *NOT* meant to be a database abstraction tool. It is assumed the user is comfortable using SQL for querying databases.**

Drop the datamgmt.py into your Python system path to install.

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
* Appending files together.
* Search a file and return any line with specific text.

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


## Sending a Delimited Data File to a Database Table

Using the **file_to_db** function, you can easily send even very large files to a database. By default, it will set all columns to varchar(250) to ensure no conflicts of data types. It will attempt to insert 100 rows at a time, but you can change this with the batchsize parameter. If you want to drop an existing table and replace it with the data from the file, use the drop_table=1 option. Otherwise if the table exists, it will append the data to that table.

**Example 1: Sending mydata.csv to the table my_table**

`file_to_db('mydata.csv',delim=',',dsn='my_odbc_connection',table='my_table')`

**Example 2: Sending mydata.csv to the table my_table, but making sure to drop the table first and reducing to only running 50 rows at a time**

`file_to_db('mydata.csv',delim=',',dsn='my_odbc_connection',table='my_table',batchsize=50,drop_table=1)`

**The file_to_db() signature:**
<pre>
file_to_db(
file='',             # The file to process
dsn='',              # The ODBC DSN connection name
table='',            # The table name to send the data
delim='\t',          # The delimiter for the file
batchsize=100,       # The number of records to process at at time.
drop_table=0,        # Attempt to drop the table first, then create table.
ignore_errors=0,     # If an error occurs, ignore and keep processing the file
colwidth=250,        # Column width (all columns will be varchar). Can increase if data is wider than 250.
clean_colnames=0,    # Remove any special characters from the column names and replace spaces with underscores
skip_file_length=0   # Do not attempt to find the number of lines in the file first, and instead process file immediately.
)
</pre>


## Querying a Database and Sending the Results to a File

Using the **querydb()** function, you can send a SQL query to a database and return the results to a delimited file. You can either set the SQL directly using the **sql** argument or have it read a SQL script using the **script** argument.

**Example: Build a SQL script and run**
<pre>
sql_code = "SELECT * FROM myTable"
querydb(sql=sql_code,dsn='my_connection',results='my_results.txt')
</pre>

**Example: Read a SQL script and run**
`querydb(script='SQL_script.sql',dsn='my_connection',results='my_results.txt')`


**The querydb() signature:**
<pre>
querydb(
sql='',            # The SQL code to run
script='',         # A SQL script file to run
dsn='',            # ODBC DSN name
file='--',         # Output file. If left as "--" the results will be put into a file results_[current_timestamp].txt
batchsize=100,     # The number of records to download at a time
delim='\t',        # Delimiter for the output file.
nofetch=0,         # Set to 1 to not fetch results. Useful if running something like an UPDATE query.
nowrite=0,         # Do not write results to file, instead return the results as a list of lists
as_dict=0,         # If nowrite=1 and as_dict=1, the results will be a list of dictionaries including the column headers.
sqlserver=0        # Set to 1 to have SQLServer use the MultiSubnetFailover=Yes option, which is sometimes needed to work.
)
</pre>


