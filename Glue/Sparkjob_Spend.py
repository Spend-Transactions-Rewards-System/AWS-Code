import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, concat, create_map, when
from pyspark.sql.types import StringType, DoubleType, StructType,StructField
import pyspark.sql.functions as f
from itertools import chain
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

import json
import csv, io, os
import requests
import mysql.connector
from functools import partial
from itertools import chain

#################################################################
# Initialize spark job
# args: <class 'dict'>
# {'job_bookmark_option': 'job-bookmark-disable', 'job_bookmark_from': None, 'job_bookmark_to': None, 'JOB_ID': 'j_566bd1edad5a2b158b8d13f2a6813a61e7b57211d4c2bd1155d0e838fc1ec417', 'JOB_RUN_ID': 'jr_dbd0b8a9652a89171f924a814d76dccfaf0b2cd114555119d72ee0c43ab065d1', 'SECURITY_CONFIGURATION': None, 'encryption_type': None, 'enable_data_lineage': None, 'RedshiftTempDir': 's3://aws-glue-assets-148484133023-ap-southeast-1/temporary/', 'TempDir': 's3://aws-glue-assets-148484133023-ap-southeast-1/temporary/', 'JOB_NAME': 'Sparkjob_Users', 'S3PATH': 'value11', 'TENANT': 'value22'}
#################################################################
args = getResolvedOptions(sys.argv, ["JOB_NAME", "filename", "TENANT", "PATCH_URL", "queueName"])
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

def processingFunc(partitionData, conversionRateMapping, mccExclusionDf, campaignRulesDf, customCatDf):
#     print("Start")
    remarks_mapping = {
        "default": ["Base reward", "Category reward", "Campaign reward"],
        "cols": ["rewardAmountBase","rewardAmountCat" , "rewardAmountCampaign"],
        "rate": ['cashback', 'miles', 'miles', 'points']
    }
    for row in partitionData:
        row = row.asDict()
        row['mcc'] = int(row['mcc'])
        row['cardTypeId'] = int(row['cardTypeId'])
        row['remark'] = ''
        print(row)
        try:
            check_exclusion = len(mccExclusionDf.loc[(mccExclusionDf['card_type_id'] == row['cardTypeId']) & (mccExclusionDf['mcc_mcc'] == row['mcc'])]) > 0
            currencyRate = conversionRateMapping.get(row['currency'].upper(), False)
            if check_exclusion:
                print( f"{row['mcc']} is part of the exclusion list!")
                row['remark'] = f"No reward due to exclusion - {row['mcc']}"
            elif not currencyRate:
                print(f"{row[currency]} is not in currency mapping!")
                row['remark'] = f"{row[currency]} is not in currency mapping!"
                row['rewardAmountBase'] = None
            else:
                amount = float(row['amount']) * currencyRate
                isForeign = row['currency'] != "SGD"
                if isForeign:
                    amount = round(amount, 0)
                
                # Calculate base, category and campaigns
                conditions_base = campaignRulesDf.loc[(campaignRulesDf['title'] == 'base') & (campaignRulesDf['card_type_id'] == row['cardTypeId'])]
                conditions_category = campaignRulesDf.loc[(campaignRulesDf['title'] == 'category') & (campaignRulesDf['card_type_id'] == row['cardTypeId'])]
                conditions_campaigns = campaignRulesDf.loc[(campaignRulesDf['end_date'].notna()) & (campaignRulesDf['card_type_id'] == row['cardTypeId'])]
                conditions_arr = [conditions_base, conditions_category, conditions_campaigns]
                
                for i in range(len(conditions_arr)):
                    conditions = conditions_arr[i]
                    reward_rate = 0
                    remark = ''
                    for index, condition in conditions.iterrows():
                        if condition['is_foreign'] and condition['is_foreign'] is not isForeign:
                            continue
                        if float(condition['min_dollar_spent']) > amount:
                            continue
                        if condition['merchant'] is not None and condition['merchant'].upper() not in row["merchant"].upper():
                            continue

                        if condition['custom_category_name'] is not None:
                            custom_cat_list = customCatDf.loc[( customCatDf.apply(lambda x: x['name'].upper() in row['merchant'].upper(), axis=1) ) & (customCatDf['mcc'] == int(row['mcc'])), 'merchant'].tolist()
                            if condition['custom_category_name'] not in custom_cat_list:
                                continue

                        if reward_rate < float(condition['reward_rate']):
                            reward_rate = float(condition['reward_rate'])
#                             {condition['title']}-
                            
                            if row['cardTypeId'] == 1:
                                rate = round(condition['reward_rate']*100,2)
                            else:
                                rate = round(condition['reward_rate'],2)
                            
                            remark =  f"{remarks_mapping['default'][i]} giving {rate} {remarks_mapping['rate'][row['cardTypeId'] - 1]}/SGD" 
                            
                    if reward_rate != 0: 
#                         print(reward_rate, row['merchant'], amount)
                        amount *= reward_rate
                        row[remarks_mapping['cols'][i]] = amount
                    row['remark'] += f"{remark},"
                        
        except Exception as e:
            print(e)
            row['remark'] = f"Error in processing, {e}"
            row['rewardAmountBase'] = None
        yield Row(**row)

def sendToQueue(partitionData, queueName, tenant):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queueName)
    
    for row in partitionData:
        row = row.asDict()
        remarks = row['remark'].split(',')
        amount_arr = [row['rewardAmountBase'],row['rewardAmountCat'],row['rewardAmountCampaign'] ]
        for i in range(3):
            if remarks[i] == '':
                continue
            date_arr = row['transaction_date'].split("/")
            if len(date_arr[1]) == 1:
                date_arr[1] = "0" + date_arr[1]
            if len(date_arr[0]) == 1:
                date_arr[0] = "0" + date_arr[0]
            date_arr = [date_arr[2], date_arr[1], date_arr[0]]
            date_string = "-".join(date_arr)
            
            message_body = {
                "tenant": tenant.upper(),
                "transactionId": row['transaction_id'],
                "transactionDate": date_string,
                "cardId": row['card_id'],
                "merchant": row['merchant'],
                "mcc": row['mcc'],
                "currency": row['currency'],
                "amount": float(row['amount']),
                "rewardAmount": round(float(amount_arr[i]),2),
                "remarks": remarks[i]
            }
            try:
                response = queue.send_message(MessageBody=json.dumps(message_body))
            except Exception as e:
                print(e)
                sqs = boto3.resource('sqs')
                queue = sqs.get_queue_by_name(QueueName=queueName)
                response = queue.send_message(MessageBody=message_body)

def get_client(resource, profile=None):
    """
    Common resources: 'cognito-idp', 's3', 'sagemaker', 'sagemaker-runtime'
    """
    if profile is None:
        return boto3.client(resource)
    else:
        session = boto3.session.Session(profile_name = profile)
        return session.client(resource)            
#################################################################
# Run ETL
#################################################################
# IMPORTANT VARIABLES
TENANT = args.get('TENANT') #'scis'
conversionRateMapping = {
    "USD": 1.35,
    "SGD": 1
}
csvFile = args.get('filename') #'./testSpend.csv'
queueName = args.get('queueName') # 'CampaignToCard'
ERROR_FOLDER = datetime.now().strftime('%Y%m%d-%H%M%S')
s3_error_path = f"s3://spend-t3-bucket/error/{ERROR_FOLDER}"
PATCH_URL = args.get('PATCH_URL')

# Get relevant connections
glue = boto3.client('glue')
campaign_db2_c = DB_Connection(db_name = 'campaign_db', glue_cilent = glue, connection_name = 'dev-card-ms-campaign_db', driver_type = "mysql", driver_port = 3306) # any connection in the vpc # DB_Connection(db_name = 'campaign_db', hostname = 'host.docker.internal', driver_type = "mysql", driver_port = 8003, user = 'admin', password = 'adminadmin')

# Read data
print("Read csv data...")
s3_df = spark.read.format("csv").option("header", "true").load(csvFile)
s3_df = s3_df.withColumn("remark", lit(None).cast(StringType()))
ORIGINAL_COUNT = s3_df.count()

#################################################################
# Validate
#################################################################
print("Validating...")
# Check MCC
errorDf = s3_df.filter(col('mcc').isNull())
s3_df = s3_df.filter(col('mcc').isNotNull())
errorDf = errorDf.na.fill(value="MCC is empty!",subset=["remark"])

# Check duplicates rows
s3_df = s3_df.join(
    s3_df.groupBy('transaction_id').agg((f.count("*")>1).cast("int").alias("Duplicate_indicator")),
    on='transaction_id',
    how="inner"
)
errorDf = errorDf.union(s3_df.filter(col("Duplicate_indicator") == 1).select(errorDf.columns))
s3_df = s3_df.filter(col("Duplicate_indicator") != 1).select(errorDf.columns)
errorDf = errorDf.na.fill(value="Duplicate rows",subset=["remark"])

# Check card type
cardTypesDf = spark.read.format("jdbc").option('driver', 'com.mysql.jdbc.Driver').option("url", campaign_db2_c.jdbc_url).option("user", campaign_db2_c.user).option("password", campaign_db2_c.password).option("dbtable", "card_type").load()
cardTypesDf = cardTypesDf.filter(col("tenant") == TENANT)
cardTypesDf = cardTypesDf.withColumnRenamed("id", "cardTypeId")

# interest cols
colsInterest = s3_df.columns
colsInterest.append("cardTypeId")
s3_df = s3_df.join(cardTypesDf, s3_df.card_type ==  cardTypesDf.name,"left").select(colsInterest)

# Check card type
errorDf = errorDf.union(s3_df.filter(col('cardTypeId').isNull()).select(errorDf.columns) )
s3_df = s3_df.filter(col('cardTypeId').isNotNull())

errorDf = errorDf.na.fill(value="Card type not registered in DB!",subset=["remark"])

#################################################################
# Process
#################################################################
print("Get data for processing...")
s3_df = s3_df.withColumn("rewardAmountBase", lit(0).cast(DoubleType()) ).withColumn("rewardAmountCat", lit(0).cast(DoubleType()) ).withColumn("rewardAmountCampaign", lit(0).cast(DoubleType()) )

# Get metadata
# Get Exclusion list
mccExclusionDf = spark.read.format("jdbc").option('driver', 'com.mysql.jdbc.Driver').option("url", campaign_db2_c.jdbc_url).option("user", campaign_db2_c.user).option("password", campaign_db2_c.password).option("dbtable", "mcc_exclusion").load()
mccExclusionDf = mccExclusionDf.filter(col("tenant") == TENANT).toPandas()

# Campaign Rules
campaignRulesDf = spark.read.format("jdbc").option('driver', 'com.mysql.jdbc.Driver').option("url", campaign_db2_c.jdbc_url).option("user", campaign_db2_c.user).option("password", campaign_db2_c.password).option("dbtable", "campaign").load()
campaignRulesDf = campaignRulesDf.filter(col("is_active")== True).toPandas()

# Custom Category
customCatDf = spark.read.format("jdbc").option('driver', 'com.mysql.jdbc.Driver').option("url", campaign_db2_c.jdbc_url).option("user", campaign_db2_c.user).option("password", campaign_db2_c.password).option("dbtable", "custom_category").load().toPandas()

# Start processing
print("Start processing...")
campaign_db2_c.close_conn()
s3_df_rdd = s3_df.rdd.mapPartitions( partial(processingFunc, conversionRateMapping = conversionRateMapping, mccExclusionDf=mccExclusionDf, campaignRulesDf=campaignRulesDf, customCatDf= customCatDf) )

deptSchema = StructType([       
    StructField('id', StringType(), True),
    StructField('transaction_id', StringType(), True),
    StructField('merchant', StringType(), True),
    StructField('mcc', StringType(), True),
    StructField('currency', StringType(), True),
    StructField('amount', StringType(), True),
    StructField('transaction_date', StringType(), True),
    StructField('card_id', StringType(), True),
    StructField('card_pan', StringType(), True),
    StructField('card_type', StringType(), True),
    StructField('remark', StringType(), True),
    StructField('cardTypeId', StringType(), True),
    StructField('rewardAmountBase', DoubleType(), True),
    StructField('rewardAmountCat', DoubleType(), True),
    StructField('rewardAmountCampaign', DoubleType(), True),
])

s3_df = spark.createDataFrame(s3_df_rdd, schema = deptSchema)

# s3_df = s3_df_rdd.toDF()

# Last validation
errorDf = errorDf.union(s3_df.filter(col('rewardAmountBase').isNull()).select(errorDf.columns) )
s3_df = s3_df.filter(col('rewardAmountBase').isNotNull())
ERROR_COUNT = errorDf.count()

# Write error Files
print("Write error file...")
errorDf.coalesce(1).write.format('csv').option("header", "true").save(s3_error_path)

s3 = get_client("s3", None)
rsp = s3.list_objects_v2(Bucket="spend-t3-bucket", Prefix=f"error/{ERROR_FOLDER}/", Delimiter="/")
try:
    objectList = list(obj["Key"] for obj in rsp["Contents"])
    for obj in objectList:
        print(obj)
        if obj[-4:] == '.csv':
            s3_error_path = f"s3://spend-t3-bucket/{obj}"
            break
except Exception:
    s3_error_path = ''

print("Send to queue...")

# Send to queue
s3_df.foreachPartition( partial(sendToQueue, queueName=queueName, tenant = TENANT) )

# Update service
data = {
    "filename": os.path.basename(csvFile),
    "numberOfProcessed": ORIGINAL_COUNT - ERROR_COUNT,
    "numberOfRejected": ERROR_COUNT,
    "errorFileURL": s3_error_path
}
print(data)
response = requests.patch(PATCH_URL, json = data)
print(vars(response))


print(response)

print("GLUE JOB DONE!")
job.commit()
