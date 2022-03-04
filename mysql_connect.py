
from statistics import mode
import sys
import logging
import os
from datetime import date
from turtle import forward
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
        5. REMEBER TO ADD NOTE THAT IN_FILE NEEDS TO BE ENABLED
"""

def parse_data(rest_of_file, table_name,windows=False):
    #Create Clean Load File:
    
    #Date Outgoing File
    today = date.today()
    str_today = today.strftime('%Y_%m_%d')

    #Handle Outgoing Directory
    dir_path = os.path.dirname(os.path.realpath(__file__)) 
    load_file = table_name + f'_{str_today}'

    if windows:
        forward_slash_file =  dir_path + mysql_config.out_dir + load_file 
        full_file = forward_slash_file.replace('\\','/')
    else:
        full_file =  dir_path + mysql_config.out_dir + load_file # Set Outgoing Directory in Config
        

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
 
    logger.info(f'Wrote output file {full_file} with {record_count} records')
    print(f'Wrote output file {full_file} with {record_count} records')

    return full_file


def connect_db(host,user,password,database): # Make it take custom args that can be passed into connect
    # Connect to MySQL
    try:
        conn = pymysql.connect(host=host, user=user, password=password, database=database, connect_timeout=5, local_infile=True)
    except BaseException  as e:
            logger.error(f'An exception occured: {e}')
            sys.exit()

    logger.info("SUCCESS: Connection to MySQL instance succeeded")
    print("SUCCESS: Connection to MySQL instance succeeded")
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
        print(f'DDL Created for {table_name}')
        logger.info(ddl)
        print(ddl)

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
    print(f"SUCCESS: Created Table {table_name}")

def load_data(conn,load_file,table_name, logger):
    # Insert Data with Load
    try:
        cursor = conn.cursor()
        load_sql = f"""LOAD DATA LOCAL INFILE '{load_file}' into table {table_name} CHARACTER SET 'UTF8' FIELDS TERMINATED BY X'01' LINES TERMINATED BY '\n';"""
        # load_sql = f"""LOAD DATA LOCAL INFILE 'E:/Downloads/MAX-Project-Backend-Data/MAX-Backend-Jami-Tefridj/artist_2022_03_03' into table artist CHARACTER SET 'UTF8' FIELDS TERMINATED BY ',' LINES TERMINATED BY '|';"""
        print(load_sql)
        results_load = cursor.execute(load_sql)
        conn.commit()
        logger.info(f'Here are the results: f{results_load}')
        print(f'Here are the results: f{results_load}')
    except BaseException  as e:
        logger.error(f'An exception occured: {e}')
        sys.exit()

    cursor.close()
    # Return Load Numbers and Date Time 

################################################################################
# Read Command Line Inputs
input_file = sys.argv[1]
table_name = str(os.path.basename(input_file)).replace('.txt','')
print(table_name)
#MySQL settings

mysql_host  = mysql_config.db_host
name = mysql_config.db_username
password = mysql_config.db_password
db_name = mysql_config.db_name

#Start Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)



conn = connect_db(mysql_host,name,password,db_name)

ddl = create_ddl_str(table_name, field_list, db_type_list)

create_table(conn,table_name,ddl,logger)

load_data(conn,load_file,table_name, logger)