# Python Data Management


import datetime
import os
import gc
import pypyodbc
import csv
import string
import xlrd

def cp(x):
    '''Copy mutable variable types so you can make them independant'''
    if type(x)==list: return list(x)
    if type(x)==dict: return dict(x)
    if type(x)==set: return set(x)
    return x

def dtnow(simple=0):
    '''Returns current date and time as a string'''
    x = str(datetime.datetime.now())[:19]
    if simple==1: # remove special characters
        removechars = ' -,:.;\t\n)(]['
        for c in removechars:
            x = x.replace(c,'')
    return x

def find_files(txt=''):
    '''Finds files with [txt] in the filename. Returns a list.'''
    if txt == '':
        print('find_files() -- No text provided.')
        return []
    txt = str(txt).lower()
    filenames = [x for x in os.listdir() if x.lower().find(txt) > -1]
    return filenames

def stringclean(x,nospecial=0):
    '''
    Cleans a string of any non-standard characters.
    Uses a character whitelist to ensure only acceptable characters
    are getting through. Non-standard characters are replaced by
    a space.
    '''
    x = str(x)
    clean = ''
    whitelist = """ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz 1234567890!@#$%^&*(){}[];|,<>?/"'\\:.\t\n-=_+`~ """
    if nospecial == 1:
        whitelist = """ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz 1234567890"""
    for char in x:
        if char in whitelist:
            clean+=char
        else:
            clean+=' '
    while clean.find('  ') > -1:
        clean = clean.replace('  ',' ')
    return clean

def int2(x):
    '''Tries to convert to int, otherwise return original'''
    try:
        y=int(x)
        return y
    except:
        return x
    return x

def div2(n,d):
    '''Try to divide n/d. Accounts for possible errors, such as division by zero.'''
    if d == 0: return 0

    try:
        n=float(n)
        d=float(d)
        result = n/d
        return result
    except:
        pass

    try:
        result = n/d
        return result
    except:
        pass

    print('Could not divide {n} / {d}'.format(n=n,d=d))
    return None

try_div = div2 # alias div2 to try_div

def cls():
    """Clear the screen (Windows Cmd Prompt)"""
    a=os.system('cls')
    return

def trim(x,chars=''):
    '''Can use instead of str.strip()'''
    s=str(x)
    if chars == '': return(s.strip())
    s=s.strip(chars)
    return s


def avg(*args):
    '''Simple Average Function'''
    total = 0
    for x in args:
        total+=x
    return total*1.0/len(args)


def append_files(files=[],results='appended_files.txt',suppress_output=0):
    '''Appends files together'''
    if files == []:
        return
    outfile=open(results,'w')
    for f in files:
        if suppress_output == 0:
            print(f)
        infile = open(f,'r')
        for line in infile:
            outfile.write(line)
        infile.close()
    outfile.close()
    return


def file_search(f,txt,results=''):
    txt=str(txt)
    if results == '':
        results='file_search_results_{ts}.txt'.format(ts=dtnow(1))
    outfile = open(results,'w')
    infile = open(f,'r')
    counter = 1
    for line in infile:
        if counter == 1:
            outfile.write(line)
            counter+=1
            continue
        if line.upper().find(txt.upper()) > -1:
            outfile.write(line)
        counter+=1
    outfile.close()




def readfile(file,start=0,limit=0):
    filename = find_files(file)

    if len(filename)>0:
        filename = filename[0]

    if len(filename) == 0:
        print('No matching file found')
        return

    f=open(filename,'r')

    print('Reading File: {f}'.format(f=filename))

    file_data = []
    counter = 1
    rows_read = 0
    for x in f:
        ok = 1
        if start > 0 and counter < start and counter != 1: ok = 0
        if limit > 0 and rows_read > limit: ok = 0

        if ok == 1:
           file_data.append(x.strip('\n'))
           rows_read += 1

        counter += 1

    f.close()
    return file_data


def splitdata(data,delim='\t',progress=1):
    '''
    Splits raw data from text file by the specified delimiter. Delimiter
    defaults to tab. Uses the csv module for splitting data.
    '''

    newdata = []

    # Convert delim to correct character if spelled out
    if delim in ['tab','tsv','tabs']: delim = '\t'
    if delim in ['comma','csv','commas']: delim = ','

    # Use Python's CSV library
    rd = csv.reader([x for x in data],delimiter=delim,quotechar='"')
    counter = 0
    for x in rd:
        newdata.append(x)
        counter+=1
        if counter%1000 == 0 and progress==1:
            print(counter)
        if counter % 10000 == 0:
            gc.collect()
    return newdata



def file_to_db(file='',dsn='',table='',delim='\t',batchsize=100,
    drop_table=0,ignore_errors=0,colwidth=250,clean_colnames=0,skip_file_length=0):

    '''
    Streams the file directly to the DB without reading into memory.
    Useful for when you need to send very large files to a DB.

    Must specify:
        - The source file you want to read. (file)
        - Delimiter for the source file. (delim)
        - The target table. (table)

    Defaults to 100 rows at a time, but can be changed with
    the batchsize argument.

    ignore_errors=1 will ignore when an error occurs and continue on. Keep in
    mind all records for that batch (batchsize rows) will be lost.

    drop_table=1 will attempt to drop the table first.

    colwidth allows you to change the table column widths. Default is 250.

    You can pass a dataset using the data argument. If this is done, reading
    a file will be skipped and it will go straight to sending the data.

    ------------------------------------------------------------------------
    EXAMPLE: STREAM A CSV TO A DATABASE
    x = dfp.dataFile().stream_to_db(
        file='c:/data/x.csv'    # Read the file c:/data/x.csv
        ,delim=','              # Set it to comma-delimited
        ,dsn='my_odbc_cnx'      # Specify the odbc dsn
        ,table='table_x'        # Specify the target table name
        ,drop_table=1           # Drop the table if exists
        ,batchsize=50           # Insert 50 records at a time
        )
    '''

    # Correct delimiters if needed
    if delim in ['tabs','tab','tsv']: delim = '\t'
    if delim in ['comma','csv']: delim = ','
    if delim in ['pipe','psv']: delim = '|'

    if file == '':
        print('No file name supplied.')
        return

    filename = find_files(file)
    if len(filename) == 0:
        return 'No file found with name: {f}'.format(f=file)

    filename = filename[0]

    if table == '':
        table = 'IMPORT_TABLE_{rn}'.format(rn=random.randint(1,9999999))
        print('No table name provided. Importing to table: {table}'.format(table=table))

    print('Connecting to DB: {dsn}'.format(dsn=dsn))
    conn = pypyodbc.connect('DSN={dsn}'.format(dsn=dsn))
    curs = conn.cursor()

    if drop_table == 1:
        try:
            curs.execute("DROP TABLE [{table}];".format(table=table))
            curs.commit()
        except:
            print('Could not drop table: {table}'.format(table=table))


    # Open the file in read mode
    try:
        infile = open(filename,'r')
    except:
        print('Could not open file')
        return

    file_length = get_file_line_count(filename) if skip_file_length == 0 else 1
    if file_length == 0:
        print('File length of zero.')
        return

    # Line counter for keeping track of read progress
    line_counter = 1
    insert_sql_batch = ''

    # Loop through file
    for line in infile:
        # Process header row and create table with those field names
        if line_counter == 1:
            rd = csv.reader([line],delimiter=delim,quotechar='"')
            linedata = [x for x in rd][0]

            c=0
            while c < len(linedata):
                linedata[c] = linedata[c].strip()
                if linedata[c] == '':
                    linedata[c] = 'COL{c}'.format(c=c)
                c+=1

            if clean_colnames == 1:
                linedata = [stringclean(x,nospecial=1).replace(' ','_').replace('__','_') for x in linedata]


            sql = "CREATE TABLE {table} ({cols});".format(
                table=table
                ,cols = ", ".join([" ["+stringclean(y).strip('"/. ').replace(']','').replace('[','')+"] VARCHAR({cw}) ".format(cw=colwidth) for y in linedata])
                )
            print("Table Creation SQL:")
            print(sql)

            try:
                curs.execute(sql)
                curs.commit()
            except:
                print('Could not create table. Probably because table already exists.')

            line_counter += 1

        # All other lines besides first line
        else:
            rd = csv.reader([line],delimiter=delim,quotechar='"')
            linedata = [x for x in rd][0]
            insert_sql = 'INSERT INTO [{table_name}] VALUES ('.format(table_name=table)
            for col in linedata:
                # Remove quotes around text and replace single quotes with double-single-quotes
                # to be SQL safe.
                cleaned = stringclean(col).strip(""" "' """).replace("'","''")
                insert_sql += "'{x}',".format(x=cleaned)
            insert_sql = insert_sql.strip(', -')+');\n'
            insert_sql_batch+=insert_sql
            pass

            line_counter +=1
            if line_counter % batchsize == 0:
                try:
                    curs.executemany(insert_sql_batch)
                    curs.commit()
                except Exception as ex:
                    print('Error occurred')
                    print(str(ex))
                    if ignore_errors == 1:
                        insert_sql_batch = ''
                        continue
                    if ignore_errors == 0: break
                insert_sql_batch = ''
                status_report = """{lc} / {fl} ({pct}%)      {f}""".format(
                    lc   = line_counter
                    ,fl  = file_length
                    ,pct = round((line_counter*1.0) / file_length * 100,1)
                    ,f   = filename[:50]
                    )
                print(status_report)
                if line_counter % (batchsize*5) == 0:
                    gc.collect()


    try:
        curs.executemany(insert_sql_batch)
        curs.commit()
    except:
        pass


    return


def get_file_line_count(f):
    try:
        infile = open(f,'r')
        linecount = len([x for x in infile])
        return linecount
    except:
        return 0



def file_header(f,n=10):
    infile = open(f,'r')
    data = []
    for x in range(n):
        line = infile.readline().replace('\n','')
        data.append(line)
    return data


def write_csv(filename,filedata,delim=','):
    '''
    Writes a CSV file from a dataset (list of lists)
    - Uses the CSV module to ensure Excel compatibility
    - Can change from CSV to other delimiter using the delim argument
    '''
    import csv
    outfile = open(filename,'w',newline='')
    wt = csv.writer(outfile,dialect='excel',delimiter=delim)
    for x in filedata:
        wt.writerow(x)
    outfile.close()
    return


def read_csv(filename,delim=','):
    '''Reads a CSV file (or other delimited text files).'''
    thefile = find_files(filename)
    if len(thefile) > 0:
        thefile = thefile[0] # Take first file that matches
    else:
        print('No matching filename found')
        return
    infile = open(thefile,'r')
    data = []
    rd = csv.reader([x for x in infile],delimiter=delim,quotechar='"')
    for x in rd:
        data.append(x)
    return data


def hash_value(x):
    '''Returns the SHA256 hash of a value'''
    import hashlib
    b=str(x).encode() #convert to bytes
    result = hashlib.sha256(b).hexdigest()
    return result


def querydb(sql='',script='',dsn='',file='--', attempts=1,batchsize=100,delim='\t',nofetch=0,nowrite=0,as_dict=0,sqlserver=0):
    '''
    Execute SQL query and return results to a file instead of RAM.
    Exports results to results.txt unless specified otherwise.
    Useful when result dataset would be very large or you want to directly
    store to disk without any further processing.
    '''

    if file == '--':
        file = 'results_{ts}.txt'.format(ts=dtnow(simple=1))

    if script != '':
        sql=readfile(script)
        try:
            sql = ' \n '.join(sql)
        except:
            pass

    print('Connecting to DB')
    pypyodbc.SQL_AUTOCOMMIT_OFF=1
    if sqlserver==1:
        sqlserver_cnx =';MultiSubnetFailover=Yes'
        pass
    conn = pypyodbc.connect('DSN='+str(dsn)+sqlserver_cnx)
    curs = conn.cursor()

    counter = 0

    print('Running Query')
    results = curs.execute(sql)

    if nofetch == 1:
        curs.commit()
        return

    print('Getting column names')
    colnames = [column[0] for column in curs.description]

    # If nowrite has been selected fetch all data and return
    if nowrite == 1:
        # Return a dictionary with column names
        dataset = curs.fetchall()
        if as_dict == 0:
            return {'columns':colnames,'data':dataset}
        # returns data in list of dictionaries format
        if as_dict == 1:
            data_dict = [{colnames[i]:dataset[c][i] for i in range(len(colnames))} for c in range(len(dataset))]
            return data_dict

    outfile = open(file,'w')

    # Write column names header
    outfile.write(delim.join(colnames)+'\n')

    rowcount = 1
    while rowcount > 0:
        temp1 = curs.fetchmany(batchsize)
        rowcount = len(temp1)
        for r in temp1:
            str_r = [str(x) for x in r]
            outfile.write(delim.join(str_r)+'\n')
        counter+=rowcount
        print(counter)

    outfile.close()
    curs.close()
    conn.close()
    print('Results in {f}'.format(f=file))
    return

def get_uuid(nodash=0):
    '''Generate a unique identifier'''
    import uuid
    x = str(uuid.uuid4())
    if nodash==1:
        x = x.replace('-','')
    return x

def get_random_number(max=999999999999):
    '''
    Generate a random number between 1 and max.
    Max defaults to 999999999999
    '''
    import random
    r = random.randint(1,max)
    return r

def dummy_data(record_length=10,row_length=200,max_val=1000):
    '''
    Generate a random list of lists.
    Used for testing functions that are to process datasets.
    '''
    data = [[0]*record_length]*row_length
    data2 = [[get_random_number(max_val) for y in x] for x in data]
    return data2

def time_24_to_ampm(x):
    '''
    Converts 24-hour times HH:MM:SS to AM/PM format
    '''
    x=str(x)
    if x is None or len(x) < 2: return x
    if x[2]!=':': return x
    if x.find(':') == -1: return x

    ampm = 'AM'
    hr = x[:2]

    if int(hr) > 12:
        hr = int(hr)-12
        ampm = 'PM'

    if hr == '12': ampm='PM'
    if hr == '00': hr = '12'

    newtime = str(hr).rjust(2,'0') + x[2:]+ampm
    return newtime


# Get data from an excel workbook
def read_excel(f="",sh="",rowstart=0,progress=0,mmddyyyy=0,ddmmyyyy=0,timeoption=0,ampm=0):
    """
    Reads an Excel file and stores data in list format.

    Parameter Definitions:
    f..............excel file path
    sh.............sheet name
    rowstart.......skip # of lines to start reading on row
    progress.......Display # of lines processed
    mmddyyyy.......Converts dates to MM/DD/YYYY format
    ddmmyyyy.......Converts dates to DD/MM/YYYY format
    time...........Include time HH:MM:SS in the date fields
    """

    if f == "":
        print("read_excel: No file specied")
        return ["No file specied"]

    # Function to convert dates to strings instead of numbers
    def convert_excel_date(x,dm):
        # x = cell_value of sheet, dm = datemode from excel workbook
        datevalue=xlrd.xldate_as_tuple(x,dm)
        dt = '%s-%s-%s'%(   # YYYY-MM-DD Format
            str(datevalue[0]),
            str(datevalue[1]).rjust(2,'0'),
            str(datevalue[2]).rjust(2,'0')
            )

        # time string
        tm = "{h}:{mm}:{s}".format(
            h = str(datevalue[3]).rjust(2,'0'),
            mm=str(datevalue[4]).rjust(2,'0'),
            s=str(datevalue[5]).rjust(2,'0')
            )

        if ampm == 1:
            tm = time_24_to_ampm(tm)

        # Convert to MM/DD/YYYY format if specified
        if mmddyyyy == 1:
            dt = '{m}/{d}/{y}'.format(m=str(datevalue[1]).rjust(2,'0'),d=str(datevalue[2]).rjust(2,'0'),y=str(datevalue[0]))

        # Convert to DD/MM/YYYY format if specified
        if ddmmyyyy == 1:
            dt = '{d}/{m}/{y}'.format(m=str(datevalue[1]).rjust(2,'0'),d=str(datevalue[2]).rjust(2,'0'),y=str(datevalue[0]))

        # Include time if specified
        if timeoption==1:
            dt = dt+" "+tm

        return dt

    xl_data = [] # Empty list to throw data
    try:
        wb = xlrd.open_workbook(f)        # Open the workbook
        if sh not in  ["","UNKNOWN",None]:
            sheet = wb.sheet_by_name(sh)  # Open the worksheet we want
        else:
            sheet = wb.sheet_by_index(0)  # If none was specified, take the first sheet in the workbook
    except:
        return 'error'


    # Now loop through the data and put into the xl_data list variable
    if rowstart > 0: rowstart -= 1
    row = rowstart if rowstart > 0 else 0                   # Row counter variable

    while row < sheet.nrows:                                # While loop to go through the ROWS of the worksheet
        rowdata = []                                        # For each row, create an empty list for that row's data
        col = 0                                             # Reset the column counter

        while col < sheet.ncols:                            # While loop to go through each COLUMN of the current row
            c = sheet.cell(row,col)                         # Assign cell to variable 'c'
            if c.ctype != 3:                                # If type is not a date:
              val = str(c.value)
              val = val.replace('.0','') if val[-2:]=='.0' else val # Remove .0 from values.
              rowdata.append(val)                           # Append values as is
            else:                                           # Else if type is date:
                try:
                    c2=sheet.cell_value(row,col)
                    dt=convert_excel_date(c2,wb.datemode)   #  1. Convert to YYYY-MM-DD Format (ISO 8601)
                    rowdata.append(dt)                      #  2. Append formatted date to row data
                except:
                    rowdata.append('Bad Date Conversion')
            col+=1                                          # Increment column counter

        xl_data.append(rowdata)                             # Now add the row's data to the UHC data
        row+=1                                              # Increment row counter
        if progress==1 and row%5000==0:
            print('%s rows'%(row))

    return xl_data # End of excel function; Returns data as list



