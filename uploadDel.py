import boto3
import botocore
import os
import sys

s3 = boto3.resource("s3")
client = boto3.client('s3', 'us-west-2')
bucket = s3.Bucket('iqcprod')


def process(line):
    try:
        s3.Object('iqcprod', 'uiqcfolder/uploaded/%s' % line).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            status = 'not in S3'

        else:
            # Something else has gone wrong.
            status = 'not in S3'
            raise
    else:
        # The object does exist.
        status = 'in S3'
        s3.Object('iqcprod', 'uiqcfolder/uploaded/%s' % line).delete()

    with open('/Users/mabuaita/Uploadlog.txt', "a") as uploadlog:
        uploadlog.write(line + '\t' + status + '\n')


with open('/Users/mabuaita/cleanUpload.txt') as f:
    for line in f:
        line = line.rstrip('\n')
        process(line)
