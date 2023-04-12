import os, sys
import boto3
import json
import time
import urllib.request


# External libraries
sys.path.append(os.path.join(os.path.dirname(__file__), "package"))
from jose import jwk, jwt
from jose.utils import base64url_decode

# ENV needed to configure
# ENV, AWS_REGION, COGNITO_USER_POOL_ID, COGNITO_CLIENT_ID

# Local testing
if os.environ.get('ENV') is None:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), "../.env") # edit this for your local env file
    assert os.path.exists(dotenv_path), f"Environment file does not exists: {dotenv_path}"
    load_dotenv(dotenv_path)

# envs
AWS_REGION = os.environ['AWS_REGION']
COGNITO_USER_POOL_ID = os.environ['COGNITO_USER_POOL_ID']
COGNITO_APP_CLIENT_ID = os.environ['COGNITO_CLIENT_ID']

keys_url = 'https://cognito-idp.{}.amazonaws.com/{}/.well-known/jwks.json'.format(AWS_REGION, COGNITO_USER_POOL_ID)
# instead of re-downloading the public keys every time
# we download them only on cold start
# https://aws.amazon.com/blogs/compute/container-reuse-in-lambda/
# https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-tokens-verifying-a-jwt.html
with urllib.request.urlopen(keys_url) as f:
    response = f.read()
keys = json.loads(response.decode('utf-8'))['keys']

ROLE_GROUP_MAPPING = {
    "admin": "test-g1"
}

def lambda_handler(event, context):
    print(event)

    token_data = parse_token_data(event) # get token
    if token_data['valid'] is False:
        return get_deny_json()

    
    try:
        claims = validate_token(token_data['token'])
        groups = claims.get('cognito:groups', [])

        # Create policy documents based on condition
        policyDocument = get_policy_document(event['methodArn'], condition = groups) # Hardcoding now (sort of, but can modify to dynamically change)
        
        return get_response_object(
            policyDocument, 
            principalId=claims['sub'], 
            context={
                "test1": claims['username']
            }
        )

    except Exception as e:
        print(e)

    return get_deny_json()


def get_policy_document(methodArn, condition):
    # https://github.com/DMalliaros/aws-python-lambda-authorizers/blob/master/src/authorizer.py
    # https://github.com/aws-samples/amazon-cognito-api-gateway/blob/80ee4cd9933c4362e30c62a9e52ac5b880d340b6/custom-auth/lambda.py#L56
    # https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-lambda-authorizer-output.html
    # https://www.alexdebrie.com/posts/lambda-custom-authorizers/
    # "arn:aws:execute-api:{regionId}:{accountId}:{apiId}/{stage}/{httpVerb}/[{resource}/[{child-resources}]]"
    methodArn = methodArn.split("arn:aws:execute-api:")[1]
    methodArn = methodArn.split("/")
    gateway_details, stage, httpVerb = methodArn[0], methodArn[1], methodArn[2]
    regionId, accountId, apiId = gateway_details.split(":")

    # Check if it in group
    is_admin = ROLE_GROUP_MAPPING["admin"] in condition
    print("is_admin:", is_admin)
    if is_admin:
        statements = [
            {
                "Action": "execute-api:Invoke",
                "Effect": "Allow",
                "Resource": f"arn:aws:execute-api:{regionId}:{accountId}:{apiId}/{stage}/*/*"
            }
        ]
    else:
        statements = [
            {
                "Action": "execute-api:Invoke",
                "Effect": "deny",
                "Resource": f"arn:aws:execute-api:{regionId}:{accountId}:{apiId}/{stage}/*/admin-only"
            },
            {
                "Action": "execute-api:Invoke",
                "Effect": "Allow",
                "Resource": f"arn:aws:execute-api:{regionId}:{accountId}:{apiId}/{stage}/*/admin-only/all"
            }
        ]
    print(statements)
    policyDocument = {"Version": "2012-10-17", "Statement": statements}
    return policyDocument


def get_response_object(policyDocument, principalId='yyyyyyyy', context={}):
    return {
        "principalId": principalId,
        "policyDocument": policyDocument,
        "context": context,
        "usageIdentifierKey": "{api-key}"
    }


def get_deny_json():
    return {
        "principalId": "yyyyyyyy",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": "Deny",
                    "Resource": "arn:aws:execute-api:*:*:*/*/ANY/*"
                }
            ]
        },
        "context": {},
        "usageIdentifierKey": "{api-key}"
    }


def parse_token_data(event):
    response = {'valid': False}

    ################## Request Authorization Method ##################
    # if 'Authorization' not in event['headers']:
    #     return response

    # auth_header = event['headers']['Authorization']
    # auth_header_list = auth_header.split(' ')

    # # deny request of header isn't made out of two strings, or
    # # first string isn't equal to "Bearer" (enforcing following standards,
    # # but technically could be anything or could be left out completely)
    # if len(auth_header_list) != 2 or auth_header_list[0] != 'Bearer':
    #     return response

    # access_token = auth_header_list[1]


    ################## Token Authorization Method ##################
    access_token = event.get('authorizationToken')
    if access_token is None:
        return response

    return {
        'valid': True,
        'token': access_token
    }


def validate_token(token):
    # get the kid from the headers prior to verification
    headers = jwt.get_unverified_headers(token)
    kid = headers['kid']

    # search for the kid in the downloaded public keys
    key_index = -1
    for i in range(len(keys)):
        if kid == keys[i]['kid']:
            key_index = i
            break

    if key_index == -1:
        print('Public key not found in jwks.json')
        return False

    # construct the public key
    public_key = jwk.construct(keys[key_index])

    # get the last two sections of the token,
    # message and signature (encoded in base64)
    message, encoded_signature = str(token).rsplit('.', 1)

    # decode the signature
    decoded_signature = base64url_decode(encoded_signature.encode('utf-8'))

    # verify the signature
    if not public_key.verify(message.encode("utf8"), decoded_signature):
        print('Signature verification failed')
        return False

    print('Signature successfully verified')

    # since we passed the verification, we can now safely
    # use the unverified claims
    claims = jwt.get_unverified_claims(token)
    
    # additionally we can verify the token expiration
    if time.time() > claims['exp']:
        print('Token is expired')
        return False
    
    # and the Audience  (use claims['client_id'] if verifying an access token) # NOT APPLICABLE FOR ID_TOKEN
    if claims['client_id'] != COGNITO_APP_CLIENT_ID:
        print('Token was not issued for this audience')
        return False

    # now we can use the claims
    print(claims)
    return claims

if __name__ == "__main__":
    # Testing
    event_obj = {
        "headers": {
            "Authorization": f'Bearer {os.environ["COGNITO_ACCESS_TOKEN"]}'
        }
    }
    print(lambda_handler(event = event_obj, context = None))
