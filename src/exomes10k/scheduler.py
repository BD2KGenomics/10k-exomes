import boto
import boto.sqs
import boto.s3.key
from boto.sqs.message import Message
import time

normal_key = None
tumor_key = None


class Connection:
    """
    has connection to SQS queues and s3 buckets, deals with those interactions
    """
    def __init__(self):
        self.sqs = boto.sqs.connect_to_region("us-west-2")

        self.connS3 = boto.connect_s3()
        # start holds the initial files, middle holds intermediates, and end holds final result
        self.start_bucket = self.connS3.get_bucket('bd2k-test-flow-start')
        self.middle_bucket = self.connS3.get_bucket('bd2k-test-flow-intermediate')
        self.end_bucket = self.connS3.get_bucket('bd2k-test-flow-final')

        self.start_queue = self.sqs.get_queue('bd2k-queue-start')
        self.int_queue = self.sqs.get_queue('bd2k-queue-intermediate')
        # We will try to read from start_queue and its corresponding bucket. If it is empty, we will
        # read from int_queue and set currentBucket to middleBucket
        self.current_bucket = self.start_bucket
        self.current_queue = self.start_queue
        self.message = self.start_queue.read()

        if self.message is None:
            self.current_queue = self.int_queue
            self.message = self.current_queue.read()
            self.current_bucket = self.middle_bucket

    def get_bucket_name(self):
        return self.current_bucket.name

    def set_global_bucket(self):
        bucket = self.current_bucket

    def get_keys(self):
        normal_key = self.current_bucket.get_key(self.message.get_body())
        tumor_key = self.current_bucket.get_key(self.message.get_body().replace("normal", "tumour"))
        return (normal_key.name, tumor_key.name)

    def upload(self, nameList):
        if nameList is None:
            return
        for name in nameList:
            # we want to put the intermediates in the second bucket
            k = boto.s3.key.Key(self.middle_bucket)
            k.key = name
            k.set_contents_from_filename(name)

    def extend_timeout(self):
        while True:
            self.message.change_visibility(5)
            time.sleep(5)


def main():
    pass


if __name__ == '__main__':
    main()



