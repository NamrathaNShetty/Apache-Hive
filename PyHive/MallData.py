from Loggers import logger
import pandas as pd
import os
from pyhive import hive
from dotenv import load_dotenv
load_dotenv(".env")

host_name = os.getenv("HOST_NAME")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")


def createConnection():
    """
    Description:
        This method is used to create a connection with hive.
    """
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database,  auth='CUSTOM')

    return conn

def create_database(db_name):
    """
    Description:
        This method is used to create a hive database.
    Parameter:
        It takes database name as a parameter for creating database.
    """
    try:
        connection = hive.Connection(host=host_name, port=port, username=user, password=password,
                                     auth='CUSTOM')

        cur = connection.cursor()
        cur.execute("CREATE DATABASE Mall_Cust")
        logger.info("Database created successfully")

    except Exception as e:
        logger.error(e)


def create_table_and_load_csv():
    """
    Description:
        This method is used to create a table in a hive database.
    """

    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("create table mall(ID int,Gender string,Age int,Roll_no int,Score int) row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/Hive/'")
        logger.info("Table has been created successfully")

    except Exception as e:
        logger.error(e)

def load__hive_data_into_panda_df():
    """
    Description:
        This method is used to create a panda dataframe by doing query with a hive database.
    """
    try:
        conn = createConnection()
        df = pd.read_sql("select * from mall where Gender ='Male'",conn)
        df.head(5)
        logger.info(df)

    except Exception as e:
        logger.error(e)    

def drop_hive_table(tbl_name):
    """
    Description:
        This method is used to delete a hive database table.
    """
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("drop table mall")
        logger.info("Table Deleted Successfully")

    except Exception as e:
        logger.error(e)


#create_database("Mall_Cust")
# create_table_and_load_csv()
load__hive_data_into_panda_df()
# drop_hive_table("mall")