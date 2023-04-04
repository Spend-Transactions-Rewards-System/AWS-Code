import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    print(event)
    print(context)
    
    glue = boto3.client('glue')
    BUCKET = event["Records"][0]["s3"]["bucket"]["name"]
    S3_KEY = event["Records"][0]["s3"]["object"]["key"]
    S3PATH = f"s3://{BUCKET}/{S3_KEY}"
    
    FOLDER = S3_KEY.split("/")[-2]
    OUTPUTFOLDER = f"error/{FOLDER}/"
    
    print(S3PATH, BUCKET, OUTPUTFOLDER)
    
    # "JOB_NAME", "S3PATH", "TENANT", "BUCKET", "OUTPUTFOLDER"
    
    try:
        if BUCKET == "user-t3-bucket":
            glue.start_job_run(
                JobName='Sparkjob_Users', 
                Arguments={
                    "--S3PATH": S3PATH,
                    "--TENANT": "scis",
                    "--BUCKET": BUCKET,
                    "--OUTPUTFOLDER": OUTPUTFOLDER,
                }
            )
        
        print("DONE")
        return {
            'statusCode': 200,
            'body': json.dumps(f"s3://{BUCKET}/{OUTPUTFOLDER}")
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 404,
            'body': json.dumps(f'{e}')
        }
