import boto3
import base64


from botocore.exceptions import ClientError

def get_most_recent_object(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    return sorted_objects[0]['Key']
