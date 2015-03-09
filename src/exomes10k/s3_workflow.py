__author__ = 'CJ'
import subprocess
from exomes10k.scheduler import Connection
from exomes10k.workflow import WorkFlow
from exomes10k.workflow import Step
import os

normal_key = None
tumor_key = None
current_bucket = None
tool_dir = os.path.dirname(__file__)
#add data_dir

class S3Workflow(WorkFlow):
    def __init__(self):
        downloadNormal = Step(command="aws s3 cp s3://{current_bucket}/{normal_key} {tool_dir}".format(**globals()),
                               inputs={None},
                               outputs={"{normal_key}".format(**globals())})

        downloadTumor = Step(command="aws s3 cp s3://{current_bucket}/{tumor_key} {tool_dir}".format(**globals()),
                               inputs={None},
                               outputs={"{tumor_key}".format(**globals())})

        self.steps = [downloadNormal, downloadTumor]

        WorkFlow.__init__(self, self.steps, tool_dir)


def main():
    connection = Connection()
    global current_bucket
    current_bucket = connection.get_bucket_name()
    global normal_key
    global tumor_key
    normal_key,tumor_key = connection.get_keys()

    workflow = S3Workflow()

    for step in workflow.steps:
        subprocess.check_call(step.command, shell=True)


if __name__ == '__main__':
    main()