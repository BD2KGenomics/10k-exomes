import time
import sys
import os
import boto
import boto.sqs
import boto.s3.key
import subprocess
from boto.exception import EC2ResponseError
from boto.sqs.message import Message
import threading
import subprocess

# globals for formatting
bucket = "bucket.txt"
normalUUID = "1-111-11111"
toyOneOutput = "toyOneOutput.txt"
toyTwoOutput = "toyTwoOutput.txt"
tumorUUID = None
workdir = '.'

class Step:
    inputs = set()
    outputs = set()
    def __init__(self, command, inputs, outputs):
        self.command = command
        self.outputs = outputs
        self.inputs = inputs


class WorkFlow:

    #tumor_bam="{tumor}.tumor.bam".format( globals() )

    #normalDownload = Step( command="aws s3 --bucket {bucket} --key {normalKey.name}",
    #                       input=[],
    #                       output=[])
    #tumorDownload = Step(command="aws s3 --bucket {bucket} --key {tumorKey.name}", None, None, None)

    toyOne = Step(command="python toyOne.py {bucket}", inputs=set(["bucket.txt"]), outputs=set(["toyOneOutput.txt"]))
    toyTwo = Step(command="python toyTwo.py {toyOneOutput}", inputs=set(["toyOneOutput.txt"]), outputs=set(["toyTwoOutput.txt"]))
    toyThree = Step(command="python toyThree.py {toyTwoOutput}", inputs=set(["toyTwoOutput.txt"]), outputs=set(["toyThreeOutput.txt"]))

    workList = [toyOne, toyTwo, toyThree]
    
    def resumption_step(self, fileSet):
        for index, step in enumerate(self.workList):
            if step.inputs.issubset(fileSet) and not step.outputs.issubset(fileSet):
                return index
        return 7

    def getUnusedFiles(self, index, fileSet):
        cannot_be_deleted = set()
        for file in fileSet:
            for step in self.workList[index:]:
                if file in step.inputs:
                    cannot_be_deleted.add(file)
        return fileSet-cannot_be_deleted


# has connection to SQS queues and s3 buckets, deals with those interactions
class Connection:
    sqs = boto.sqs.connect_to_region("us-west-2")
    connS3 = boto.connect_s3()
    # start holds the initial files, middle holds intermediates, and end holds final result
    startBucket = connS3.get_bucket('bd2k-test-flow-start')
    middleBucket = connS3.get_bucket('bd2k-test-flow-intermediate')
    endBucket = connS3.get_bucket('bd2k-test-flow-final')

    start_queue = sqs.get_queue('bd2k-queue-start')
    int_queue = sqs.get_queue('bd2k-queue-intermediate')
    # We will try to read from start_queue and its corresponding bucket. If it is empty, we will
    # read from int_queue and set currentBucket to middleBucket
    currentBucket = startBucket
    currentQueue = start_queue
    message = start_queue.read()

    if message is None:
        currentQueue = int_queue
        message = currentQueue.read()
        currentBucket = middleBucket

    def __init__(self):
        pass

    def getBucket(self):
        return self.currentBucket

    def setGlobalBucket(self):
        bucket = self.currentBucket

    def getKey(self):
        normalUUID = self.currentBucket.get_key(self.message.get_body())
        tumorUUID = self.currentBucket.get_key(self.message.get_body().replace("normal", "tumour"))

    def upload(self, nameList):
        if nameList is None:
            return
        for name in nameList:
            # we want to put the intermediates in the second bucket
            k = boto.s3.key.Key(self.middleBucket)
            k.key = name
            k.set_contents_from_filename(name)


def getExistingFiles():
    files = set(f for f in os.listdir(workdir) if os.path.isfile(f))
    #files.add('bucket.txt')
    return files

def main():
    workflow = WorkFlow()
    print(getExistingFiles())
    print(workflow.workList[0].inputs)
    print(set(['bucket.txt']).issubset(getExistingFiles()))
    print(workflow.workList[0].inputs.issubset(getExistingFiles()))
    # we want to start at the correct step in the list
    for step in workflow.workList[:]:
        command = step.command.format(**globals())
        subprocess.check_call(command, shell=True)
        index = workflow.resumption_step(getExistingFiles())
        print(index)
        print(workflow.getUnusedFiles(index=index, fileSet=getExistingFiles()))


if __name__ == '__main__':
    main()



