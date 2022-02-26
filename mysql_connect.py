import sys
import logging
import mysql_config
import pymysql

print("STARTING")
#MySQL settings

mysql_host  = "auroradbmysql.crmqojacvx6b.us-east-2.rds.amazonaws.com"
name = mysql_config.db_username
password = mysql_config.db_password
db_name = mysql_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)


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