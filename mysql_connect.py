from fileinput import filename
from statistics import mode
import sys
import logging
import os
import mysql_config
import pymysql
import re

"""
NOTES: 1. Remember to use this as docstring first and foremost
        2. Running list of arg parse params
            a. --e --env (dev,qa,prod)
            b. --f --file-extension (default txt)
            c. --multi MULTIPLE FILES STILL NEED TO DESIGN
        3. Log Files and how to set up logger.info with timestampes
        4. FUNCTIONS WITH TRY CATCH AND ERROR HANDLING
"""
print("STARTING")

# Read Command Line Inputs
input_file = sys.argv[1]

#MySQL settings

mysql_host  = "auroradbmysql.crmqojacvx6b.us-east-2.rds.amazonaws.com"
name = mysql_config.db_username
password = mysql_config.db_password
db_name = mysql_config.db_name

#Start Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Parse File
print(input_file)

table_name = str(input_file).replace('.txt','') #Would want to make this smartere to read other file extensions --SET AS PARAMATER IN CLI

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
db_types_list = db_types.split('\x01') 

# Possible Expansion is handling multiple tables in create statement
# -- Assumption -- that field and db_type list would always be same length
field_len = len(field_list)
i = 0

# Create DDL SQL 
ddl = f'CREATE TABLE `{table_name}`('
while i < field_len:
    ddl+='\n `' + field_list[(i)] + '` '+ db_types_list[i]+','
    i+=1
ddl+='\nPRIMARY KEY (`' + pk + '`)'
ddl+=');'

logger.info(f'DDL Created for {table_name}')
logger.info(ddl)
print(ddl)

#CSV File to make into function
rest_of_file

# Connect to MySQL
try:
    conn = pymysql.connect(host=mysql_host, user=name, password=password, database=db_name, connect_timeout=5)
    print(conn)
    conn.close()
    print(conn)
except:
    # NEED BETTER ERROR LOGGING for REAL EXAMPLES
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    sys.exit()

logger.info("SUCCESS: Connection to MySQL instance succeeded")

# Create Table

# Insert Data with Load

# Return Load Numbers and Date Time 
