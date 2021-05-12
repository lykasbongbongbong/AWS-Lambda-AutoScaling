# tested under py37 function
import json
import time
import sys
import getopt
import os
import logging
import uuid
import math
import boto3
import urllib.request
import botocore

##########################################################
# Connect to SQS and poll for messages
##########################################################
def lambda_handler(event, context):

    '''init part'''
    AWS_ACCESS_KEY_ID = 'ASIAROHPZOUO4PXVRCO5'
    AWS_SECRET_ACCESS_KEY = '0FljjQZA0QdMPkPeeJ/mpkVga+sOf9UfOKBiILed'
    AWS_SESSION_TOKEN = 'IQoJb3JpZ2luX2VjECAaCXVzLXdlc3QtMiJGMEQCIAE+PyMDom22GzDVrCZdRMId+JzGMRuZu+0Jl2zkDFiaAiBhkCzz2+xHu9dvu4yq+ElD7yYHu8IEnDVH1vgOPyVC1Sq9Agip//////////8BEAAaDDA5OTI4NzEzNTUxNyIM4SLn0jP15lOUyHHcKpECVcTrb0llyHGxKTV/t2kaTk+F34EXWvtoUzCo+H++Gm6tiFAmbduqDTzDAUXUVU69a+A8cglUfQK4aXOsVc4UWqycV2N2xKsDeF4ZnS6k/5itd2D3qbgNxE3E8vbGVy/LoSzwiNt5oz67TfN6zJWfA4Cqxvx6aqWKzMNAjzcNSx2ZxJQJ/KakN1pCCjaIX0AVXC0FAUEk34DIx9ARUFIkMKvYb+MWsaIExSP8X+sNOmwc4nQ8jKX9oSSz3LWVOsCXEXzizK+VqDs+MClRFyOQJawsk4iEitfEmapDsxVmnP6twCVsV0VYaWn7pm4X8V6cnavd+uZCRseq7r5p7LvYSGr93Bx2FMFz0o0MM4m1+64NMInE6oQGOp4BkUsCw3/FaW6fJIOqXT8XGbQiwH/Ae3ii7QxXzzbFz8d8boSS9O46x6vvG7i347PBL5n6FoHROq0YpcfZeduz5WX34JPE8p2c01DgRvXD9z3ASDz708UfH4vLktO2M5ted/gHHHo6uYEN1mL7AHdgo4prnXSjQEnxkPd0kMtjt4vreFCVgySrTe4+k+e61CcDXS62v6JAVFrir0uxxyM='
    # print(f"input queue name: {event['input_queue']}")
    # print(f"output queue name: {event['output_queue']}")
    # print(f"s3 output bucket name: {event['s3_output_bucket']}")
    # print(f"region name: {event['region']}")

    # Get region
    region_name = event['region']
    # # If no region supplied, extract it from meta-data
    # if region_name == '':
    #     conn = httplib.HTTPConnection("169.254.169.254", 80)
    #     conn.request("GET", "/latest/meta-data/placement/availability-zone/")
    #     response = conn.getresponse()
    #     region_name = response.read()[:-1]
    # print("Using Region: " + region_name)

    # Set queue names
    input_queue_name = event['input_queue']
    output_queue_name = event['output_queue']

    # sqs_client = boto3.client('sqs', region_name=region_name, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)
    sqs_client = boto3.client('sqs')
    sqs = boto3.resource('sqs')
    s3_client = boto3.client('s3')

    
	# Get S3 bucket, create if none supplied
    s3_output_bucket_name = event['s3_output_bucket']
    if s3_output_bucket_name == "":
        s3_output_bucket_name = "hw3-output-bucket"
        s3_client.create_bucket(Bucket=s3_output_bucket_name)
        s3_client.put_bucket_policy(
            Bucket=s3_output_bucket_name,
            Policy='{"Version": "2012-10-17","Statement": [{"Sid": "PublicReadForGetBucketObjects","Effect": "Allow","Principal": "*","Action": "s3:GetObject","Resource": "arn:aws:s3:::hw3-output-bucket/*"}]}',
        )
    
    print('Retrieving jobs from queue %s. Processed images will be stored in %s and a message placed in queue %s' % (input_queue_name, s3_output_bucket_name, output_queue_name))

    #connect to sqs and open queue
    input_queue_url_response = sqs_client.get_queue_url(QueueName=input_queue_name)
    output_queue_url_response = sqs_client.get_queue_url(QueueName=output_queue_name)
    input_queue_url = input_queue_url_response["QueueUrl"]
    output_queue_url = output_queue_url_response["QueueUrl"]

    print("Input queue url: " + input_queue_url)
    print("Output queue url: "+ output_queue_url)
    print("Polling input queue...")
    
    while True:
        # Get the queue
        queue = sqs.get_queue_by_name(QueueName=input_queue_name)
        for msg in queue.receive_messages():
            print("Message received...")
            message = msg.body 
            # print()
            # print(message)
            # print()
            receipt_handle = msg.receipt_handle

            #Create a unique job id
            job_id = str(uuid.uuid4())

            # Process the image, creating the image montage
            output_url = process_message(message, s3_output_bucket_name, job_id)  
            print(output_url)  

            # time.sleep(15) 
            output_message = "Output available at: %s" % (output_url) 

            #write message to output queue
            write_output_message(sqs_client, output_message, output_queue_url)

            print(output_message)
            print("Image processing completed")

            # Delete message from the queue           
            sqs_client.delete_message(QueueUrl=input_queue_url, ReceiptHandle=receipt_handle)
        
            break
        break

##############################################################################
# Process a newline-delimited list of URls
##############################################################################
def process_message(message, s3_output_bucket_name, job_id):
    # output_dir = "/home/ec2-user/jobs/%s/" % (job_id)
    output_dir = "/tmp/" + job_id
    output_image_name = "output-%s.png" % (job_id)
    # output_image_path = output_dir + output_image_name 
    output_image_path = "/tmp/" + job_id + "/" + output_image_name

    os.chdir("/tmp")

    #create job_id folder
    mkdir_cmd = "mkdir /tmp/"+job_id
    print("Make a dir at: " + mkdir_cmd)
    os.system(mkdir_cmd)

    # cd to job_id folder
    job_id_folder = "/tmp/"+job_id
    os.chdir(job_id_folder)
    # cd_to_job_id_folder_cmd = "cd /tmp/"+job_id
    print("Change Folder to " + job_id_folder)

    for line in message.splitlines():
        img_name = line.split('/')[-1]
        urllib.request.urlretrieve(line, img_name)

    # # Invoke ImageMagick to create a montage
    print("-----Doing montage-----")
    tmp_img_path = output_dir + "/" + "20120728-DSC01292-L.jpg"
    montage_cmd = "montage -size 400x400 null: " + output_dir + "/* null: -thumbnail 400x400 -bordercolor white -background black +polaroid -resize 80% -gravity center -background black -geometry -10+2  -tile x1 " + output_image_path

    print(montage_cmd)
    os.system(montage_cmd)
    #write the resuling image to s3
    print("-----Start write to s3-----")

    # output_image_name = "20120728-DSC01292-L.jpg"
    output_image_path = "/tmp/"+job_id+"/"+output_image_name
    
    output_url = write_image_to_s3(output_image_path, s3_output_bucket_name, output_image_name)
    print(output_url)
    return output_url

##############################################################################
# Write an image to S3
##############################################################################
def write_image_to_s3(output_image_path, s3_output_bucket_name, output_image_name):
    s3 = boto3.resource('s3')
    # Boto3
    bucket = s3.Bucket(s3_output_bucket_name)
    exists = True
    try:
        s3.meta.client.head_bucket(Bucket=s3_output_bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = e.response['Error']['Code']
        if error_code == '404':
            exists = False
    s3_client = boto3.client('s3')

    print("-----start uploading file-----")
    print("Output Image Path: "+output_image_path)
    print("s3 output bucket name: " + s3_output_bucket_name)
    print("Output Image Name: "+output_image_name)
    reponse = s3_client.upload_file(output_image_path, s3_output_bucket_name, output_image_name)
	# Return a URL to the object
    output_url = "https://" + s3_output_bucket_name + ".s3.amazonaws.com/"+output_image_name
    return output_url 

##############################################################################
# Write the result of a job to the output queue: OK
##############################################################################		
def write_output_message(sqs_client, message, output_queue_url):
    response = sqs_client.send_message(
        QueueUrl=output_queue_url,
        MessageBody=message,
        DelaySeconds=1,
    )
    
	