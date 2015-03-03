import time
import sys
import boto
import boto.sqs
import subprocess
from boto.exception import EC2ResponseError
from boto.sqs.message import Message
import threading
import subprocess

normalFileName = None
tumourFileName = None


class ToolSerialization:
    def __init__(self, tool, functionInput, functionOutput, remove):
        self.tool = tool
        self.output = functionOutput
        self.functionInput = functionInput


class WorkFlow:
    workArray = []


class Connection:
    sqs = boto.sqs.connect_to_region("us-west-2")
    connS3 = boto.connect_s3()

    startBucket = connS3.get_bucket('bd2k-test-flow-start')
    middleBucket = connS3.get_bucket('bd2k-test-flow-intermediate')
    endBucket = connS3.get_bucket('bd2k-test-flow-final')

    start_queue = sqs.get_queue('bd2k-queue-start')
    int_queue = sqs.get_queue('bd2k-queue-intermediate')
    #We will try to read from start_queue and its corresponding bucket. If it is empty, we will
    #read from int_queue and set currentBucket to middleBucket
    currentBucket = startBucket
    currentQueue = start_queue
    message = start_queue.read()

    def __init__(self):
        self.startStep = 0

    if message is None:
        currentQueue = int_queue
        message = currentQueue.read()
        currentBucket = middleBucket

    def getBucket(self):
        return self.currentBucket

    def getStartStep(self):
        return self.startStep

    def setGlobals(self):
        normalFileName = "100-{}".format(self.message.get_body())
        tumourFileName = normalFileName.replace("normal", "tumour")

    def download(self):
        normalKey = self.currentBucket.get_key(self.message.get_body())
        tumourKey = self.currentBucket.get_key(self.message.get_body().replace("normal", "tumour"))
        normalKey.get_contents_to_filename(normalFileName)
        normalKey.get_contents_to_filename(tumourFileName)

    def upload(self, nameList):
        for name in nameList:
            k=Key(self.currentBucket)
            k.key = name
            k.set_contents_from_filename(name)

def main():
    connection = Connection()
    workflow = WorkFlow()
    connection.setGlobals()
    connection.download()

    for step in workflow.workArray[connection.startStep:]:
        tool = step.tool
        tool = tool.format(globals())
        subprocess.check_call( tool, shell=True )
        connection.upload(tool.output)

if __name__ == '__main__':
    main()



