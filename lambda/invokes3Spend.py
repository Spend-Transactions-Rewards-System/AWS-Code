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
    
    print(S3PATH)
    try:
        if BUCKET == "spend-t3-bucket":
            glue.start_job_run(
                JobName='Sparkjob_Spend', 
                Arguments={
                    "--filename": S3PATH,
                    "--TENANT": "scis",
                    "--PATCH_URL": "http://internal-custom-alb-867640773.ap-southeast-1.elb.amazonaws.com:8080/api/v1/upload/glue",
                    "--queueName": 'CampaignToCard',
                }
            )
        
        print("DONE-")
        return {
            'statusCode': 200,
            'body': json.dumps(f"{S3PATH}")
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 404,
            'body': json.dumps(f'{e}')
        }
