import sys
import time
#from time import gmtime, strftime
import datetime
from datetime import datetime, timedelta, timezone
import boto3
import botocore
import os

DicomFolder = '/DICOM/'
log = '/home/mddx/logs/filedel.txt'
region = 'us-west-2'
now = time.time()
cutoff = now - (2 * 86400)
#print (strftime("%Y-%m-%d %H:%M:%S", gmtime()))
print (time.ctime())

def bucket(bucketEnv):
	s3 = boto3.resource("s3")
	bucket = s3.Bucket(bucketEnv)
	exists = True
	try:
		s3.meta.client.head_bucket(Bucket=bucketEnv)
	except botocore.exceptions.ClientError as e:
		# If a client error is thrown, then check that it was a 404 error.
		# If it was a 404 error, then the bucket does not exist.
		error_code = int(e.response['Error']['Code'])
		if error_code == 404:
			exists = "false"
		return exists

def iqcenv():
	try:
#		os.environ[iqcenv]
		env = os.environ["iqcenv"]
		return env
	except KeyError:
		print ("environment: dev, stage, or prod, not set")
		sys.exit(1)

def delCandidate(bucket):
	s3 = boto3.resource("s3")
	client = boto3.client('s3', 'us-west-2')
	buckets = s3.buckets.all()
	epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

	arr = []
	for root, dirs, files in os.walk(DicomFolder):
		for fname in files:
			filepath = os.path.join(root, fname)
			stat = os.stat(filepath)
			tstat = stat.st_ctime
			modified = epoch + timedelta(seconds=tstat)
			if tstat < cutoff:
				try:
					s3.Object(bucket, 'uiqcfolder/uploaded/%s' % fname).load()
				except botocore.exceptions.ClientError as e:
					if e.response['Error']['Code'] == "404":
						# The object does not exist.
						status = 'not in S3'
						client.upload_file(filepath, bucket, 'uiqcfolder/uploaded/%s' % fname)
					else:
						# Something else has gone wrong.
						status = 'not in S3'
						client.upload_file(filepath, bucket, 'uiqcfolder/uploaded/%s' % fname)
						raise
				else:
					# The object does exist.
					status = 'in S3'
					os.remove(filepath)
				with open(log, "a") as logfile:
					logfile.write(filepath + '\t' + str(modified) + '\t' +  status +'\n')
	with open(log, "a") as logfile:
		logfile.write(str(time.ctime()) + '\n')
		logfile.write("=====================================================================================" + '\n')
		logfile.close() 

def main():
#	env = str(os.environ[iqcenv])
#	print (os.environ[iqcenv])
	env = iqcenv()
	if env == None:
		print ("iqcenv: test, stage, or prod, not set")
		sys.exit(1)
	bucketEnv = ('iqc%s' % env)
	exists = bucket(bucketEnv)
	if exists == "false":
		print ("bucket does not exist")
		sys.exit(1)
	delCandidate(bucketEnv)

if __name__ == "__main__":
	main()
