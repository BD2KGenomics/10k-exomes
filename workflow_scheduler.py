import time
import sys
import boto
import boto.sqs
import boto.s3.key
import subprocess
from boto.exception import EC2ResponseError
from boto.sqs.message import Message
import threading
import subprocess

# globals for formatting
bucket = None
normalKey = None
tumorKey = None


class ToolSerialization:
    def __init__(self, tool, functionInput, functionOutput, remove):
        self.tool = tool
        self.output = functionOutput
        self.functionInput = functionInput


class WorkFlow:
    normalDownload = ToolSerialization("aws s3 --bucket {bucket} --key {normalKey}", None, None, None)
    tumorDownload = ToolSerialization("aws s3 --bucket {bucket} --key {tumorKey}", None, None, None)
    workArray = [normalDownload, tumorDownload]


class Connection:
    sqs = boto.sqs.connect_to_region("us-west-2")
    connS3 = boto.connect_s3()

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
    startStep = 0

    if message is None:
        currentQueue = int_queue
        message = currentQueue.read()
        currentBucket = middleBucket

    def __init__(self):
        pass

    def getBucket(self):
        return self.currentBucket

    def getStartStep(self):
        return self.startStep

    def setGlobals(self):
        bucket = self.currentBucket

    def getKey(self):
        normalKey = self.currentBucket.get_key(self.message.get_body())
        tumourKey = self.currentBucket.get_key(self.message.get_body().replace("normal", "tumour"))


    def upload(self, nameList):
        for name in nameList:
            k = boto.s3.key.Key(self.currentBucket)
            k.key = name
            k.set_contents_from_filename(name)


def main():
    connection = Connection()
    workflow = WorkFlow()
    connection.getKey()
    connection.setGlobals()
    # we want to start at the correct step in the array
    for step in workflow.workArray[connection.startStep:]:
        tool = step.tool
        tool = tool.format(globals())
        subprocess.check_call(tool, shell=True)
        connection.upload(tool.output)


if __name__ == '__main__':
    main()



