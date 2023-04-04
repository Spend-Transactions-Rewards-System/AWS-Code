#################################################################
# Import
#################################################################
import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, concat, create_map
from pyspark.sql.types import StringType
from itertools import chain
from awsglue.context import GlueContext
from awsglue.job import Job

import csv, io
import mysql.connector
from functools import partial
from itertools import chain

#################################################################
# Initialize spark job
# args: <class 'dict'>
# {'job_bookmark_option': 'job-bookmark-disable', 'job_bookmark_from': None, 'job_bookmark_to': None, 'JOB_ID': 'j_566bd1edad5a2b158b8d13f2a6813a61e7b57211d4c2bd1155d0e838fc1ec417', 'JOB_RUN_ID': 'jr_dbd0b8a9652a89171f924a814d76dccfaf0b2cd114555119d72ee0c43ab065d1', 'SECURITY_CONFIGURATION': None, 'encryption_type': None, 'enable_data_lineage': None, 'RedshiftTempDir': 's3://aws-glue-assets-148484133023-ap-southeast-1/temporary/', 'TempDir': 's3://aws-glue-assets-148484133023-ap-southeast-1/temporary/', 'JOB_NAME': 'Sparkjob_Users', 'S3PATH': 'value11', 'TENANT': 'value22'}
#################################################################
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3PATH", "TENANT", "BUCKET", "OUTPUTFOLDER"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(args)
#################################################################
# Classes and Functions
#################################################################

class DB_Connection():
    def __init__(self, db_name, hostname = None, user = None, password = None, driver_type = "mysql", driver_port = 3306, glue_cilent = None, connection_name = None):
        try:
            self.glue = False
            if glue_cilent is not None and connection_name is not None:
                self.glue = True
                connection = glue_cilent.get_connection(Name=connection_name)
                self.hostname = connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
                self.hostname = self.hostname.split('/')[2].split(':')[0]
                self.user = connection['Connection']['ConnectionProperties']['USERNAME']
                self.password = connection['Connection']['ConnectionProperties']['PASSWORD']
            else:
                self.hostname = hostname
                self.user = user
                self.password = password
            
            self.conn = None
            self.db_name = db_name
            self.driver_type = driver_type
            self.driver_port = driver_port
            
            self.jdbc_url = f'jdbc:{self.driver_type}://{self.hostname}:{self.driver_port}/{self.db_name}'
        except Exception as e:
            print(f"Error getting connection settings for DB: {db_name} at {hostname}\n{e}")
    def get_options(self, table):
        return {'url': self.jdbc_url,'user': self.user, 'password': self.password, 'dbtable': table}
    def get_conn(self, dbType = "mysql", mandatory = False, ssl_disabled= True):
        # self.conn.is_connected()
        if dbType == "mysql":
            self.conn =  mysql.connector.connect(user=self.user, password=self.password, host=self.hostname, database=self.db_name, ssl_disabled= ssl_disabled)
        if mandatory: assert self.conn is not None, f"Did not manage to connect to {dbType}"
        return self.conn
    def close_conn(self):
        if self.conn is not None and self.conn.is_connected():
            self.conn.close()

def write_to_csv(output, filename):
    output = output.getvalue().split('\r\n')[:-1]
    if len(output) > 0:
        # print(output)
        output = [x.split(',') for x in output]
        with open(filename, 'w') as out_file:
            writer = csv.writer(out_file, delimiter =",")
            writer.writerows(output)

def db1_customer_f_partition(partitionData, db1, bucket = None, outputfolder = None):
    output = io.StringIO()
    errorExist = False
    for row in partitionData:
        if db1.conn is None or not db1.conn.is_connected():
            db1.get_conn(dbType = 'mysql')
        db1_cursor = db1.conn.cursor()
        
        row = row.asDict()
        try: # %(emp_no)s
            mapping = {'customer_id': row['id'], 'email': row['email'], 'tenant': row['TENANT']}
            db1_cursor.execute("""
                INSERT INTO customer(customer_id, email, tenant) 
                VALUES (%(customer_id)s,%(email)s,%(tenant)s) 
                ON DUPLICATE KEY UPDATE email = %(email)s;""" 
                , mapping)
            db1.conn.commit()
            # csv.DictWriter(output, row.keys()).writerow(row)
        except Exception as e: 
            print(e)
            errorExist = True
            csv.DictWriter(output, row.keys()).writerow(row)
        # yield Row(**row)
    db1.close_conn()
    if errorExist:
        print( outputfolder + 'card_customer_error.csv')
        # write_to_csv(output, 'error1.csv') # local testing
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, outputfolder + 'card_customer_error.csv').put(Body=output.getvalue())
    print("DONE - db1_customer_f_partition")

def db1_card_f_partition(partitionData, db1, bucket = None, outputfolder = None):
    output = io.StringIO()
    errorExist = False
    for row in partitionData:
        if db1.conn is None or not db1.conn.is_connected():
            db1.get_conn(dbType = 'mysql')
        db1_cursor = db1.conn.cursor()
        
        row = row.asDict()
        try: # %(emp_no)s
            mapping = {'CARD_ID': row['card_id'], 'CARD_TYPE': row['card_type'], 'REWARD_TYPE': row['REWARD_TYPE'], 'TENANT': row['TENANT'], 'CUSTOMER_ID': row['id']}
            db1_cursor.execute("""
                INSERT INTO card(card_id, card_type, reward_type, tenant, customer_id) 
                VALUES (%(CARD_ID)s,%(CARD_TYPE)s,%(REWARD_TYPE)s,%(TENANT)s,%(CUSTOMER_ID)s) 
                ON DUPLICATE KEY UPDATE card_type = %(CARD_TYPE)s, reward_type = %(REWARD_TYPE)s, customer_id = %(CUSTOMER_ID)s;""" 
                , mapping)
            db1.conn.commit()
            # csv.DictWriter(output, row.keys()).writerow(row)
        except Exception as e: 
            print(e)
            errorExist = True
            csv.DictWriter(output, row.keys()).writerow(row)
        # yield Row(**row)
    db1.close_conn()
    if errorExist:
        # write_to_csv(output, 'error1.csv') # local testing
        print(outputfolder + 'card_card_error.csv')
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, outputfolder + 'card_card_error.csv').put(Body=output.getvalue())
    print("DONE - db1_card_f_partition")

def db2_cardtype_f_partition(partitionData, db1, bucket = None, outputfolder = None):
    output = io.StringIO()
    errorExist = False
    for row in partitionData:
        if db1.conn is None or not db1.conn.is_connected():
            db1.get_conn(dbType = 'mysql')
        db1_cursor = db1.conn.cursor(buffered=True)
        
        row = row.asDict()
        try: # %(emp_no)s
            mapping = {'card_type': row['card_type'], 'tenant': row['TENANT']}
            db1_cursor.execute("""SELECT * FROM card_type where name=%(card_type)s AND tenant=%(tenant)s """, mapping)
            if db1_cursor.rowcount > 0 :
                db1_cursor.execute("""UPDATE card_type SET name=%(card_type)s , tenant=%(tenant)s WHERE name=%(card_type)s AND tenant=%(tenant)s""", mapping)
            else:
                db1_cursor.execute("""INSERT INTO card_type (name,tenant) VALUES (%(card_type)s , %(tenant)s )""", mapping )
            db1.conn.commit()
            # csv.DictWriter(output, row.keys()).writerow(row)
        except Exception as e: 
            print(e)
            errorExist = True
            csv.DictWriter(output, row.keys()).writerow(row)
        # yield Row(**row)
    db1.close_conn()
    if errorExist:
        # write_to_csv(output, 'error1.csv') # local testing
        print(outputfolder + 'campaign_cardType_error.csv')
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, outputfolder + 'campaign_cardType_error.csv').put(Body=output.getvalue())
    print("DONE - db2_cardtype_f_partition")

def db2_customer_f_partition(partitionData, db1, bucket = None, outputfolder = None):
    output = io.StringIO()
    errorExist = False
    for row in partitionData:
        if db1.conn is None or not db1.conn.is_connected():
            db1.get_conn(dbType = 'mysql')
        db1_cursor = db1.conn.cursor(buffered=True)
        
        row = row.asDict()
        try: 
            mapping = {'email': row['email'], 'name': row['name'], 'phone_number': row['phone'], 'card_type_id': row['id'], 'card_type': row['card_type']}
            # print(mapping)
            if mapping["card_type_id"] is None:
                raise Exception ("No card type ID detected!")
            db1_cursor.execute("""SELECT * FROM customer where card_type_id=%(card_type_id)s AND name=%(name)s """, mapping)
            # print(db1_cursor.rowcount, "asd")
            if db1_cursor.rowcount > 0 :
                db1_cursor.execute("""UPDATE customer SET email=%(email)s, phone_number=%(phone_number)s WHERE card_type_id=%(card_type_id)s AND name=%(name)s""", mapping)
            else:
                db1_cursor.execute("""INSERT INTO customer (email,name,phone_number,card_type_id) VALUES (%(email)s , %(name)s , %(phone_number)s, %(card_type_id)s)""", mapping )
            db1.conn.commit()
            csv.DictWriter(output, row.keys()).writerow(row)
        except Exception as e: 
            print(e)
            errorExist = True
            csv.DictWriter(output, row.keys()).writerow(row)
        # yield Row(**row)
    db1.close_conn()
    if errorExist:
        # write_to_csv(output, 'error1.csv') # local testing
        print(outputfolder + 'campaign_customer_error.csv')
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket, outputfolder + 'campaign_customer_error.csv').put(Body=output.getvalue())
    print("DONE - db2_customer_f_partition")
    
#################################################################
# Run ETL
#################################################################
# Set variables and connections ["JOB_NAME", "S3PATH", "TENANT", "BUCKET", "OUTPUTFOLDER"]
REWARD_TYPE_MAPPING = {"scis_shopping": "points", "scis_freedom": "cashback", "scis_platinummiles": "miles", "scis_premiummiles": "miles"}
s3_path = args.get('S3PATH')
TENANT = args.get('TENANT')
BUCKET = args.get('BUCKET')
OUTPUTFOLDER = args.get('OUTPUTFOLDER')
print(s3_path, TENANT, BUCKET, OUTPUTFOLDER)

glue = boto3.client('glue')
card_db1_c = DB_Connection(db_name = 'card_db', glue_cilent = glue, connection_name = 'aurora-prod-db-instance-1', driver_type = "mysql", driver_port = 3306) # any connection in the vpc
campaign_db2_c = DB_Connection(db_name = 'campaign_db', glue_cilent = glue, connection_name = 'dev-card-ms-campaign_db', driver_type = "mysql", driver_port = 3306) # any connection in the vpc

# Read data to spark dataframe
s3_df = spark.read.format("csv").option("header", "true").load(s3_path)
# s3_df = s3_df.limit(30) # Testing

# create name and tenant column in s3_df (set-up)
s3_df = s3_df.drop(*["created_at","card_pan"])
s3_df = s3_df.withColumn('name', concat(col("first_name"), lit(" "), col("last_name")))
s3_df = s3_df.withColumn('TENANT', lit(TENANT))
s3_df = s3_df.drop(*["first_name","last_name"])
mapping_expr = create_map([lit(x) for x in chain(*REWARD_TYPE_MAPPING.items())])
s3_df = s3_df.withColumn('REWARD_TYPE', mapping_expr[col("card_type")] )

# Setup dataframe to insert to the respective databases -4: CUSTOMER_db1_df, CARD_db1_df, CARDTYPE_db2_df, CUSTOMER_db2_df
CUSTOMER_db1_df = s3_df.drop_duplicates(subset=['id', 'TENANT']).select(["id", "email", "TENANT"]) # .withColumn('isProcess', lit(0))
CARD_db1_df = s3_df.drop_duplicates(subset=['card_id', 'TENANT']).select(["card_id", "card_type", "REWARD_TYPE", "TENANT", "id"]) #.withColumn('isProcess', lit(0))
CARDTYPE_db2_df = s3_df.drop_duplicates(subset=['card_type', 'TENANT']).select(["card_type", "TENANT"])
CUSTOMER_db2_df = s3_df.drop_duplicates(subset=['name', 'card_type']).select(["email", "name", "phone", "card_type"])

# join foreign key to customer via card_type_id
db2_CARDTYPE_df = spark.read.format("jdbc").option('driver', 'com.mysql.jdbc.Driver').option("url", campaign_db2_c.jdbc_url).option("user", campaign_db2_c.user).option("password", campaign_db2_c.password).option("dbtable", "card_type").load()
db2_CARDTYPE_df = db2_CARDTYPE_df.withColumnRenamed("name", "name_cardType")
CUSTOMER_db2_df = CUSTOMER_db2_df.join(db2_CARDTYPE_df, CUSTOMER_db2_df.card_type ==  db2_CARDTYPE_df.name_cardType, "left")

#################################################################
# Insert into Databases and output error file
#################################################################
print(f"Inserting to databases.... ERROR FOLDER: {OUTPUTFOLDER}")
# Start Insertion for Card Database: card and customer table
card_db1_c.conn = None
CUSTOMER_db1_df.foreachPartition( partial(db1_customer_f_partition, db1=card_db1_c, bucket = BUCKET, outputfolder = OUTPUTFOLDER) )
card_db1_c.close_conn()
print("Insert CARD DB, CUSTOMER TABLE!")

card_db1_c.conn = None
CARD_db1_df.foreachPartition( partial(db1_card_f_partition, db1=card_db1_c , bucket = BUCKET, outputfolder = OUTPUTFOLDER) )
card_db1_c.close_conn()
print("Insert CARD DB, CARD TABLE!")

# Start Insertion for Campaign Database: card and customer table
campaign_db2_c.conn = None
CARDTYPE_db2_df.foreachPartition( partial(db2_cardtype_f_partition, db1=campaign_db2_c, bucket = BUCKET, outputfolder = OUTPUTFOLDER) )
campaign_db2_c.close_conn()
print("Insert CAMPAIGN DB, cardtype TABLE!")

campaign_db2_c.conn = None
CUSTOMER_db2_df.foreachPartition( partial(db2_customer_f_partition, db1=campaign_db2_c, bucket = BUCKET, outputfolder = OUTPUTFOLDER) )
campaign_db2_c.close_conn()
print("Insert CAMPAIGN DB, customer TABLE!")


print("DONE")

job.commit()