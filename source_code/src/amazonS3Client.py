import os
import sys
import uuid
import time
import shutil
import boto3
import botocore
from datetime import datetime

# Bucket initialization
def initialize(bucketName):
	s3Client = boto3.client('s3')

	if (len(sys.argv) != 2):
		print("Utilization : python3 amazonS3Clinet.py in")
		return -1

	s3Client.create_bucket(Bucket=bucketName)
	print("The bucket was created")
	return s3Client

#Upload files
def upload(s3Client, bucketName):
	filesDir = sys.argv[1]

	for file in os.listdir("./" + filesDir):
		print("Start upload file " + file)

		start = time.time()
		s3Client.put_object(Bucket=bucketName, Key=file, Body=open(filesDir + file, "rb"))	
		end = time.time()

		print("Finish!")
		
		delta = end - start
		print("Time upload: {}\n".format(delta))
	
# Download files
def download(s3Resource, bucketName):
	downloadTimeArray = []

	if not os.path.exists("out"):
		os.makedirs("out")

	for fileD in s3Resource.Bucket(bucketName).objects.all():
		try:
			print("Start download file " + fileD.key)
			
			start = time.time()
			s3Resource.Bucket(bucketName).download_file(fileD.key, "out/" + fileD.key + "_download")
			end = time.time()

			print("Finish!")

			delta = end - start
			print("Time download: {}\n".format(delta))

		except botocore.exceptions.ClientError as e:
			if e.response["Error"]["Code"] == "404":
				print("File not found!")
			else:
				raise

# Delete bucket
def clean(s3Resource, bucketName):
	bucket = s3Resource.Bucket(bucketName)

	bucket.objects.delete()
	bucket.delete()
	shutil.rmtree("out")
	print("\nThe bucket and its contents have been deleted!")

def main():
	currentTime = datetime.now()
	dateFormatted = currentTime.strftime("%d-%m-%Y--%H-%M-%S")
	print("Run date: ", dateFormatted)
	bucketName = "big-data-app-" + dateFormatted
	print("Nume bucket: ", bucketName)
	
	s3Resource = boto3.resource("s3")

	s3Client = initialize(bucketName)

	#for i in range(0, 5):		
	upload(s3Client, bucketName)
	download(s3Resource, bucketName)

	clean(s3Resource, bucketName)

main()
