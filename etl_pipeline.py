# Install or Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id 
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import sqlite3
import psycopg2
from dotenv import load_dotenv
import os

## set jave home to avoid java running with the previous version
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-1.8'

# initialize my spark seesion
spark = SparkSession.builder\
        .appName("Globus bank Etl ")\
        .config("spark.jars", "postgresql-42.7.4.jar")\
        .getOrCreate()

# Extract this historical data into spark dataframe
df = spark.read.csv(r'dataset\rawdata\Globus_bank_transactions.csv', header=True, inferSchema=True)

# Data cleaning and Transformation(to check missing values and nulls in the column)
for column in df.columns:
    print(column,'Nulls:', df.filter(df[column].isNull()).count())

    df_clean = df.fillna({
    'Customer_Name': 'unknown',
    'Customer_Address': 'unknown',
    'Customer_City': 'unknown',
    'Customer_State':'unknown',
    'Customer_Country': 'unknown',
    'Company': 'unknown',
    'Job_Title': 'unknown',
    'Email': 'unknown',
    'Phone_Number': 'unknown',
    'Credit_Card_Number': 0,
    'IBAN': 'unknown',
    'Currency_Code': 'unknown',
    'Random_Number': 0.0,
    'Category': 'unknown',
    'Group': 'unknown',
    'Is_Active': 'unknown',
    'Description': 'unknown',
    'Gender': 'unknown',
    'Marital_Status': 'unknown'
})
    
df_clean = df_clean.na.drop(subset=['Last_Updated'])

# confirm that you have sorted missing values
for column in df_clean.columns:
    print(column,'Nulls:', df_clean.filter(df_clean[column].isNull()).count())

# Transformation to 2NF
# Transactaction table
Transaction = df_clean.select('Transaction_Date', 'Amount', 'Transaction_Type')\
                      .withColumn('Transaction_ID', monotonically_increasing_id())\
                      .select('Transaction_ID','Transaction_Date', 'Amount', 'Transaction_Type')

Transaction.show(5)

# Customer Table
Customer = df_clean.select('Customer_Name','Customer_Address','Customer_City',
                                        'Customer_State','Customer_Country').distinct()\
                   .withColumn('Customer_ID', monotonically_increasing_id())\
                   .select('Customer_ID','Customer_Name','Customer_Address','Customer_City',
                                        'Customer_State','Customer_Country')

Customer.show(5)

# Employee table
Employee = df_clean.select('Company','Job_Title','Email','Phone_Number','Gender','Marital_Status').distinct()\
                   .withColumn('Employee_ID', monotonically_increasing_id())\
                   .select('Employee_ID','Company','Job_Title','Email','Phone_Number','Gender','Marital_Status')

Employee.show(5)

Fact_table = df.join(Customer, ['Customer_Name','Customer_Address','Customer_City',\
                                        'Customer_State','Customer_Country'], 'inner')\
                         .join(Transaction, ['Transaction_Date', 'Amount', 'Transaction_Type'],'inner')\
                         .join(Employee, ['Company','Job_Title','Email','Phone_Number','Gender','Marital_Status'], 'inner')\
                         .select('Transaction_ID','Customer_ID','Employee_ID','Credit_Card_Number','IBAN',\
                                 'Currency_Code','Random_Number','Category','Group','Is_Active','Last_Updated','Description')   


Fact_table.show(5)

# DATA LOADING
load_dotenv()

def get_db_connection():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    return connection

# Establish connection
conn = get_db_connection()

# create table schema in postgresql
def create_table():
    cursor = conn.cursor()

    queries = '''
        DROP TABLE IF EXISTS Customer;
        DROP TABLE IF EXISTS Transaction;
        DROP TABLE IF EXISTS Employee;
        DROP TABLE IF EXISTS Fact_table;

        CREATE TABLE Customer (
            Customer_ID BIGINT,
            Customer_Name VARCHAR(1000),
            Customer_Address VARCHAR(1000),
            Customer_City VARCHAR(1000),
            Customer_State VARCHAR(1000),
            Customer_Country VARCHAR(1000)
        );

        CREATE TABLE Transaction (
            Transaction_ID BIGINT,
            Transaction_Type VARCHAR(1000),
            Amount FLOAT,
            Transaction_Date DATE
        );

        CREATE TABLE Employee (
            Employee_ID BIGINT,
            Company VARCHAR(1000),
            Job_Title VARCHAR(1000),
            Email VARCHAR(1000),
            Phone_Number VARCHAR(1000),
            Gender VARCHAR(1000),
            Marital_Status VARCHAR(1000)
        );

        CREATE TABLE Fact_table (
            Customer_ID BIGINT,
            Transaction_ID BIGINT,
            Employee_ID BIGINT,
            Credit_Card_Number BIGINT,
            IBAN VARCHAR(1000),
            Currency_Code VARCHAR(1000),
            Random_Number FLOAT,
            Category VARCHAR(1000),
            "Group" VARCHAR(1000),
            Is_Active VARCHAR(1000),
            Last_Updated DATE,
            Description VARCHAR(1000)
        );
    '''

    # Split the queries by semicolon and execute each one
    for query in queries.split(';'):
        query = query.strip()
        if query:  # avoid executing empty strings
            cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

    create_table()

 # Load environment variables from .env file
load_dotenv()

# Retrieve credentials from environment variables
url = os.getenv("DB_URL")
properties = { 
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER")
}

# Writing data to tables
Customer.write.jdbc(url=url, table="Customer", mode="append", properties=properties)
Transaction.write.jdbc(url=url, table="Transaction", mode="append", properties=properties)
Employee.write.jdbc(url=url, table="Employee", mode="append", properties=properties)
Fact_table.write.jdbc(url=url, table="Fact_table", mode="append", properties=properties)

print('Database, Table, and data loaded successfully')