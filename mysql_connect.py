
from statistics import mode
import sys
import logging
import os
from datetime import date
import mysql_config
import pymysql
import re

"""
NOTES: 1. Remember to use this as docstring first and foremost
        2. Running list of arg parse params
            I. --p --path (file with full path, 1st Param)
            a. --e --env (dev,qa,prod)
            b. --f --file-extension (default txt)
            c. --multi MULTIPLE FILES STILL NEED TO DESIGN
        3. Log Files and how to set up logger.info with timestampes
        4. FUNCTIONS WITH TRY CATCH AND ERROR HANDLING
"""
print("STARTING")
#FINISHED METHODS
def parse_data(rest_of_file, table_name):
    #Create Clean Load File:
    #Date Time for Outgoing File Naming -- Enhancement use date time in order to handle multiple files in a day
    # NEED TO ADD OUTGOIN DIR IN PROJECT
    today = date.today()
    str_today = today.strftime('%Y_%m_%d')
    dir_path = os.path.dirname(os.path.realpath(__file__))
    full_file =  dir_path + '\\outgoing-files\\' + table_name + f'_{str_today}'
    load_file = 'outgoing-files/' + table_name + f'_{str_today}' # would put the outgoing-files // All directories in global config -- make note
    record_count = 0

    with open(full_file, mode='w', encoding="utf-8") as writer: 
        for record in rest_of_file:
            #skipping non data entries
            if record[:7] == '##legal':
                continue
            
            # clean_record = re.sub(r'[\x01]',',',record) # replace commas
            # row_entry = re.sub(r'[\x02]','',clean_record) # remove end characters
            writer.write(record)
            record_count += 1

    logger.info(f'Wrote output file {load_file} with {record_count} records')
    print(f'Wrote output file {load_file} with {record_count} records')

    return load_file


def connect_db(host,user,password,database): # Make it take custom args that can be passed into connect
    # Connect to MySQL
    try:
        conn = pymysql.connect(host=host, user=user, password=password, database=database, connect_timeout=5, local_infile=True)
    except BaseException  as e:
            logger.error(f'An exception occured: {e}')
            sys.exit()

    logger.info("SUCCESS: Connection to MySQL instance succeeded")
    return conn


def create_ddl_str(table_name, field_list, db_type_list):
    # Create DDL SQL 
    # Possible Expansion is handling multiple tables in create statement
    # -- Assumption -- that field and db_type list would always be same length, if time allows check for it

    field_len = len(field_list)
    i = 0
    
    try:
        ddl = f'CREATE TABLE `{table_name}`('
        while i < field_len:
            ddl+='\n `' + field_list[(i)] + '` '+ db_type_list[i]+','
            i+=1
        ddl+='\nPRIMARY KEY (`' + pk + '`)'
        ddl+=');'

        logger.info(f'DDL Created for {table_name}')
        logger.info(ddl)

    except BaseException  as e:
        logger.error(f'An exception occured: {e}')
        sys.exit()
        

    return ddl

def create_table(conn, table_name, ddl, logger):
    # Create Table
    # NEED TO WRAP IN CHECK TO SEE IF THE TABLE ALREADY EXISTS
    try:
        cursor = conn.cursor()
        results = cursor.execute(ddl)
        logger.info(f'Here are the results: f{results}')
    except BaseException  as e:
        logger.error(f'An exception occured: {e}')
        sys.exit()

    cursor.close()
    logger.info(f"SUCCESS: Created Table {table_name}")

################################################################################
# Read Command Line Inputs
input_file = sys.argv[1]
table_name = str(os.path.basename(input_file)).replace('.txt','')

#MySQL settings

mysql_host  = "auroradbmysql.crmqojacvx6b.us-east-2.rds.amazonaws.com"
name = mysql_config.db_username
password = mysql_config.db_password
db_name = mysql_config.db_name

#Start Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parse File

with open(input_file, mode='r', encoding="utf-8") as reader:
    tbl_info = reader.readlines()
    # Break up file into ddl &  data
    ddl_list = tbl_info[:4]
    rest_of_file = tbl_info[4:]

# regex sub bad characters with empty charcters
clean_fields = re.sub(r'[#\n\x02]','',tbl_info[0]) 

raw_pk = re.sub(r'[#\n\x02]','',tbl_info[1])
pk = raw_pk.replace('primaryKey:', '')

clean_types = re.sub(r'[#\n\x02]','',tbl_info[2])
db_types = clean_types.replace('dbTypes:', '')

# iterate the fields to list on delim
field_list = clean_fields.split('\x01') 
db_type_list = db_types.split('\x01') 

load_file = parse_data(rest_of_file, table_name)

conn = connect_db(mysql_host,name,password,db_name)

# ddl = create_ddl_str(table_name, field_list, db_type_list)

# create_table(conn,table_name,ddl,logger)

def load_data(conn,load_file,table_name, logger):
    # Insert Data with Load
    try:
        cursor = conn.cursor()
        load_sql = f"""LOAD DATA LOCAL INFILE '{load_file}' into table {table_name} FIELDS TERMINATED BY '\x01' LINES TERMINATED BY '\x02';"""
        print(load_sql)
        results_load = cursor.execute(load_sql)
        logger.info(f'Here are the results: f{results_load}')
        print(f'Here are the results: f{results_load}')
    except BaseException  as e:
        logger.error(f'An exception occured: {e}')
        sys.exit()

    cursor.close()
    # Return Load Numbers and Date Time 
load_data(conn,load_file,table_name, logger)