import os
import unittest
import subprocess

from exomes10k.workflow import WorkFlow
from exomes10k.workflow import Step


bucket = "bucket.txt"
normalUUID = "1-111-11111"
toyOneOutput = "toyOneOutput.txt"
toyTwoOutput = "toyTwoOutput.txt"
tumorUUID = None
workdir = os.path.dirname(__file__)

# FIXME: use tempfile.mkdtemp to create temporary directory for workDir

class ToyWorkflow(WorkFlow):
    toyOne = Step(
        command="python toyOne.py {bucket}",
        inputs={"bucket.txt"},
        outputs={"toyOneOutput.txt"})
    toyTwo = Step(
        command="python toyTwo.py {toyOneOutput}",
        inputs={"toyOneOutput.txt"},
        outputs={"toyTwoOutput.txt"})
    toyThree = Step(
        command="python {toolsDir}/toyThree.py {toyTwoOutput}", # FIXME:
        inputs={"toyTwoOutput.txt"},
        outputs={"toyThreeOutput.txt"})

    steps = [toyOne, toyTwo, toyThree]

    def __init__(self):
        WorkFlow.__init__(self, self.steps, workdir)


class TestWorkFlow(unittest.TestCase):
    def setUp(self):
        os.chdir(workdir)

    def test_simple(self):
        workflow = ToyWorkflow()
        # we want to start at the correct step in the list
        for i, step in enumerate(workflow.steps):
            index = workflow.current_step()
            self.assertEqual(index, i)
            command = step.command.format(**globals())
            subprocess.check_call(command, shell=True)
            index = workflow.current_step()
            self.assertEquals(index, i + 1 )
            workflow.deletable()
