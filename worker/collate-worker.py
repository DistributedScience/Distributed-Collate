from __future__ import print_function
import boto3
import glob
import json
import logging
import os
import re
import subprocess
import sys
import time
import watchtower
import string

#################################
# CONSTANT PATHS IN THE CONTAINER
#################################

DATA_ROOT = '/home/ubuntu/bucket'
LOCAL_OUTPUT = '/home/ubuntu/local_output'
QUEUE_URL = os.environ['SQS_QUEUE_URL']
AWS_BUCKET = os.environ['AWS_BUCKET']
LOG_GROUP_NAME= os.environ['LOG_GROUP_NAME']
if 'CHECK_IF_DONE_BOOL' not in os.environ:
    CHECK_IF_DONE_BOOL = False
else:
    CHECK_IF_DONE_BOOL = os.environ['CHECK_IF_DONE_BOOL']
if 'EXPECTED_NUMBER_FILES' not in os.environ:
    EXPECTED_NUMBER_FILES = 1
else:
    EXPECTED_NUMBER_FILES = int(os.environ['EXPECTED_NUMBER_FILES'])
if 'MIN_FILE_SIZE_BYTES' not in os.environ:
    MIN_FILE_SIZE_BYTES = 1
else:
    MIN_FILE_SIZE_BYTES = int(os.environ['MIN_FILE_SIZE_BYTES'])
if 'USE_PLUGINS' not in os.environ:
    USE_PLUGINS = 'False'
else:
    USE_PLUGINS = os.environ['USE_PLUGINS']
if 'NECESSARY_STRING' not in os.environ:
    NECESSARY_STRING = False
else:
    NECESSARY_STRING = os.environ['NECESSARY_STRING']
if 'DOWNLOAD_FILES' not in os.environ:
    DOWNLOAD_FILES = False
else:
    DOWNLOAD_FILES = os.environ['DOWNLOAD_FILES']


#################################
# CLASS TO HANDLE THE SQS QUEUE
#################################

class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL

    def readMessage(self):
        response = self.client.receive_message(QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(QueueUrl=self.queueURL, ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(QueueUrl=self.queueURL, ReceiptHandle=handle, VisibilityTimeout=60)
        return

#################################
# AUXILIARY FUNCTIONS
#################################


def monitorAndLog(process,logger):
    while True:
        output= process.stdout.readline().decode()
        if output== '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)

def printandlog(text,logger):
    print(text)
    logger.info(text)

#################################
# RUN SOME PROCESS
#################################

localIn = '/workspace'

def runSomething(message):
    #List the directories in the bucket- this prevents a strange s3fs error
    rootlist=os.listdir(DATA_ROOT)
    for eachSubDir in rootlist:
        subDirName=os.path.join(DATA_ROOT,eachSubDir)
        if os.path.isdir(subDirName):
            trashvar=os.system('ls '+subDirName)

    # Configure the logs
    logger = logging.getLogger(__name__)

    group_to_run = message["group"]
    groupkeys = list(group_to_run.keys())
    groupkeys.sort()
    groupvals = [group_to_run[x] for x in groupkeys]
    metadataID = '-'.join(groupvals)

    # Add a handler with
    watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=str(metadataID),create_log_group=False)
    logger.addHandler(watchtowerlogger)

    # See if this is a message you've already handled, if you've so chosen
    # First, build a variable called remoteOut that equals your unique prefix of where your output should be
    # Then check if there are too many files

    remote_with_bucket = message['aws_remote']
    remote_prefix_only = remote_with_bucket[(remote_with_bucket[5:].index('/')+6):]

    remoteOut = f"{remote_prefix_only}/backend/{message['batch']}/{message['group']['plate']}"

    if CHECK_IF_DONE_BOOL.upper() == 'TRUE':
        try:
            printandlog(f"Checking prefix {remoteOut} in bucket {AWS_BUCKET}")
            s3client=boto3.client('s3')
            bucketlist=s3client.list_objects(Bucket=AWS_BUCKET,Prefix=remoteOut+'/')
            objectsizelist=[k['Size'] for k in bucketlist['Contents']]
            objectsizelist = [i for i in objectsizelist if i >= MIN_FILE_SIZE_BYTES]
            if NECESSARY_STRING:
                if NECESSARY_STRING != '':
                    objectsizelist = [i for i in objectsizelist if NECESSARY_STRING in i]
            if len(objectsizelist)>=int(EXPECTED_NUMBER_FILES):
                printandlog('File not run due to > expected number of files',logger)
                logger.removeHandler(watchtowerlogger)
                return 'SUCCESS'
        except KeyError: #Returned if that folder does not exist
            pass

    # Build and run your program's command
    cmd = f"python pycytominer/cyto_utils/collate_cmd.py {message['batch']} \
        {message['config']} {message['group']['plate']} --tmp-dir {message['tmp_dir']} \
        --base {message['base_directory']} --csv-dir {message['csv_dir']} \
        --image-feature-categories {message['image_feature_categories']} "
    #handle None values
    if message["column"].lower() != "none":
        cmd += f" --column {message['column']}"  
    if message["aws_remote"].lower() != "none":
        cmd += f" --aws-remote {message['aws_remote']}"  
    #handle store_true flags
    if message["aggregate_only"].lower() == "true":
        cmd += " --aggregate-only"
    if message["overwrite"].lower() == "true":
        cmd += " --overwrite"
    if message["munge"].lower() == "true":
        cmd += " --munge"
    #handle store_false flags
    if message["add_image_features"].lower() == "false":
        cmd += " --dont-add-image-features"
    if message["printtoscreen"].lower() == "false":
        cmd += " --printtoscreen"

    print('Running', cmd)
    logger.info(cmd)
    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    monitorAndLog(subp,logger)

    # Figure out a done condition - a number of files being created, a particular file being created, an exit code, etc.
    # If done, get the outputs and move them to S3

    printandlog('DONE',logger)
    return 'SUCCESS'

#################################
# MAIN WORKER LOOP
#################################

def main():
    queue = JobQueue(QUEUE_URL)
    # Main loop. Keep reading messages while they are available in SQS
    msg, handle = queue.readMessage()
    if msg is not None:
        result = runSomething(msg)
        if result == 'SUCCESS':
            print('Batch completed successfully.')
            queue.deleteMessage(handle)
        else:
            print('Returning message to the queue.')
            queue.returnMessage(handle)
    else:
        print('No messages in the queue')

#################################
# MODULE ENTRY POINT
#################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print('Worker started')
    main()
    print('Worker finished')
