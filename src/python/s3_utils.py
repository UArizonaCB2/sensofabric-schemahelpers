import os
from urllib.parse import urlparse

import boto3


# e.g. downloadDirectoryFroms3('mydatahelps','paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file')
def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucketName)
    for obj in bucket.objects.filter(Prefix=remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key)  # save to same path


def get_s3_file_paths(path):
    path_uri = urlparse(path, allow_fragments=False)
    paths = []
    client = boto3.client("s3")
    bucket = path_uri.netloc
    paginator = client.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=path_uri.path[1:])
    for page in page_iterator:
        for obj in page["Contents"]:
            paths.append(f's3://{bucket}/{obj["Key"]}')
    return paths


def get_s3_file_content(path):
    path_uri = urlparse(path, allow_fragments=False)
    bucket = path_uri.netloc
    prefix = path_uri.path[1:]
    client = boto3.client("s3")
    result = client.get_object(Bucket=bucket, Key=prefix)
    text = result["Body"].read().decode()
    return text
