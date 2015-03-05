import boto
import boto.sqs
import boto.s3.key
from boto.sqs.message import Message



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


def main():
    pass


if __name__ == '__main__':
    main()



