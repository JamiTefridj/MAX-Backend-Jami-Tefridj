import sys
import logging
import os
import argparse
from datetime import datetime,date
import mysql_config
import pymysql
import re

""" MySQL File Loader

This script takes in input file matching the file format provided creates a table for the file and loads all non #legal: data entries into table. 

The script takes the following parameters from the user
            PATH (file with full path, 1st Param)
            --e --env (dev,qa,prod)
            --f --file_ext (File Extension if there is one,default txt)
            --w --windows (Boolean if script is being run on windows machine, default True)

This script requires a config file called mysql_config.py to be filled out with MySQL connection information for dev, qa, and prod environments and outgoing file directory 
(outgoing files being cleaned source files that are loaded into the db).

The logs are written out to logs/ directory.

The script requires 'pymysql' be installed within the Python environment you are using. Additionally, IN_FILE needs to be enabled in the MySQL Database
where the files will be loaded to.

"""
def import_mysql_config(env):
    """MySQL settings -- user input handles different envs"""
    if env == 'dev':
        mysql_host  = mysql_config.dev['db_host']
        name = mysql_config.dev['db_username']
        password = mysql_config.dev['db_password']
        db_name = mysql_config.dev['db_name']
    elif env == 'qa':
        mysql_host  = mysql_config.qa['db_host']
        name = mysql_config.qa['db_username']
        password = mysql_config.qa['db_password']
        db_name = mysql_config.qa['db_name']
    elif env == 'prod':
        mysql_host  = mysql_config.prod['db_host']
        name = mysql_config.prod['db_username']
        password = mysql_config.prod['db_password']
        db_name = mysql_config.prod['db_name']
    else:
        logger.error('No proper MySQL Environment set!')
        sys.exit()
    
    return mysql_host, name, password, db_name

def parse_input(input_file,logger):
    # Parse Input File
    try:
        with open(input_file, mode='r', encoding="utf-8") as reader:
            tbl_info = reader.readlines()
            # Seperate rest of file from ddl info
            rest_of_file = tbl_info[4:]

        # regex sub delims to comma, replace header with empty
        fields = re.sub(r'[#\n\x02]','',tbl_info[0]) 
        pks = re.sub(r'[#\n\x02]','',tbl_info[1]).replace('primaryKey:', '')
        db_types = re.sub(r'[#\n\x02]','',tbl_info[2]).replace('dbTypes:', '')

        # iterate the fields to list on delim
        field_list = fields.split('\x01')
        pk_list = pks.split('\x01')
        db_type_list = db_types.split('\x01') 
        logger.info('SUCCESS: Parsed DDL & Seperated Input Data')
    except BaseException  as e:
        logger.error(f'An exception occured in parse_input(): {e}')
        sys.exit()
    
    return rest_of_file, field_list, pk_list, db_type_list

def create_load_file(rest_of_file, table_name,logger,windows=False):
    #Create Clean Load File
    
    #Date Outgoing File
    today = date.today()
    str_today = today.strftime('%Y_%m_%d')

    try:
        #Handle Outgoing Directory
        dir_path = os.path.dirname(os.path.realpath(__file__)) 
        load_file = table_name + f'_{str_today}'

        #Windows vs Linux Directories
        if windows:
            forward_slash_file =  dir_path + mysql_config.out_dir + load_file # Set Outgoing Directory in Config
            full_file = forward_slash_file.replace('\\','/')
        else:
            full_file =  dir_path + mysql_config.out_dir + load_file # Set Outgoing Directory in Config
            
        #Write File
        record_count = 0
        with open(full_file, mode='w', encoding="utf-8") as writer: 
            for record in rest_of_file:
                #skipping non data entries
                if record[:7] == '##legal':
                    continue
                
                writer.write(record)
                record_count += 1
    
        logger.info(f'SUCCESS: Wrote output file {full_file} with {record_count} records')

    except BaseException  as e:
        logger.error(f'An exception occured in create_load_file(): {e}')
        sys.exit()

    return full_file, record_count

def connect_db(host,user,password,database,logger):
    # Connect to MySQL
    try:
        conn = pymysql.connect(host=host, user=user, password=password, database=database, connect_timeout=5, local_infile=True) # Local_Infile for "LOAD DATA LOCAL INFILE"
    except BaseException  as e:
            logger.error(f'An exception occured in connect_db(): {e}')
            sys.exit()

    logger.info("SUCCESS: Connection to MySQL instance succeeded")
    return conn

def create_ddl_str(table_name, field_list, pk_list, db_type_list,logger):
    # Create DDL SQL 

    field_len = len(field_list)
    type_len = len(db_type_list)
    pk_len = len(pk_list)   

    field_i = 0
    pk_i = 0
    
    #Check that every field has a db type
    if field_len != type_len:
        logger.error(f'Field List Length {field_len} does not match DB Type Length {db_type_list}')
        sys.exit()
    
    #Create string of ddl create statement
    try:
        ddl = f'CREATE TABLE `{table_name}`('

        while field_i < field_len:
            ddl+='\n `' + field_list[(field_i)] + '` '+ db_type_list[field_i]+','
            field_i+=1
        ddl+='\nPRIMARY KEY ('

        while pk_i < pk_len:
            ddl+='`' + pk_list[(pk_i)] + '`'
            if pk_i+1 < pk_len:  # If last pk field do not add comma 
                ddl+=','
            pk_i+=1
        ddl+='));'

        logger.info(f'SUCCCES:DDL Created for {table_name}')
        logger.info(ddl)
    except BaseException  as e:
        logger.error(f'An exception occured in create_ddl_str(): {e}')
        sys.exit()
    
    return ddl

def create_table(conn, table_name, ddl, logger):
    # Create Table -- Enhancement to check if table already exists
    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
    except BaseException  as e:
        logger.error(f'An exception occured in create_table(): {e}')
        sys.exit()

    cursor.close()
    logger.info(f"SUCCESS: Created Table {table_name}")

def load_data(conn,load_file, file_count,table_name, logger):
    # Insert Data with Load
    try:
        cursor = conn.cursor()
        load_sql = f"""LOAD DATA LOCAL INFILE '{load_file}' into table {table_name} CHARACTER SET 'UTF8' FIELDS TERMINATED BY X'01' LINES TERMINATED BY '\n';"""
        logger.info(f'MySQL Load Statement: {load_sql}') 
        
        load_cnt = cursor.execute(load_sql)
        conn.commit()
        
        #Verifying Load Counts equal outgoing file count
        if load_cnt == file_count:
            logger.info(f'SUCCESS:{load_cnt} records loaded and matched input count') 
        else:
            logger.warning(f'WARNING:MISMATCH COUNTS: load_cnt: {load_cnt} != {file_count}')
        
    except BaseException  as e:
        logger.error(f'An exception occured in load_data(): {e}')
        sys.exit()

    cursor.close()

################################################################################
# Read Command Line Inputs

arg_parser = argparse.ArgumentParser(description='Load Input File into MySQL')

arg_parser.add_argument('Path',
                        metavar='P',
                        help='input file path',
                        type=str)

arg_parser.add_argument('--env',
                        '--e',
                        help='MySQL Env',
                        type=str,
                        default='dev'
                        )

arg_parser.add_argument('--file_ext',
                        '--f',
                        help='File Extension if there is one',
                        type=str,
                        default='txt'
                        )

arg_parser.add_argument('--windows',
                        '--w',
                        help='Boolean if script is being run on windows machine',
                        type=str,
                        default=True
                        )

args = arg_parser.parse_args()
input_file = args.Path
file_ext = args.file_ext
env = args.env
windows = args.windows

table_name = str(os.path.basename(input_file)).replace(f'.{file_ext}','')

#Start Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s_handler = logging.StreamHandler()
f_handler = logging.FileHandler('logs/load_data.log')
logger.addHandler(s_handler)
logger.addHandler(f_handler)

#MAIN Functionality -- could be wrapped in main function depending on implementation
now = datetime.now()
logger.info('/----------------------------------------------------------------------/\n')
logger.info(f'Starting Job for Table {table_name} at {now}')

#Input File Parsing/Output File Generation
rest_of_file, field_list, pk_list, db_type_list = parse_input(input_file,logger)
ddl = create_ddl_str(table_name, field_list, pk_list, db_type_list,logger)
load_file, record_count = create_load_file(rest_of_file, table_name,logger,windows=windows)

#MySql Tasks
mysql_host, name, password, db_name = import_mysql_config(env)
conn = connect_db(mysql_host,name,password,db_name,logger)
create_table(conn,table_name,ddl,logger)
load_data(conn,load_file, record_count, table_name, logger)
conn.close()