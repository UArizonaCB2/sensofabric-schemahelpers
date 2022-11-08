import boto3
import os 

def downloadDirectoryFroms3(bucketName, remoteDirectoryName):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucketName) 
    for obj in bucket.objects.filter(Prefix = remoteDirectoryName):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key) # save to same path


# downloadDirectoryFroms3('mydatahelps','paraquet_exports/SleepStudy/fitbit_intraday_activities_heart_file')
# downloadDirectoryFroms3('mydatahelps','paraquet_exports/SleepStudy/healthkitv2samples')