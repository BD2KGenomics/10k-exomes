import os
import unittest
import subprocess
import tempfile
import shutil

from exomes10k.workflow import WorkFlow
from exomes10k.workflow import Step


bucket = "bucket.txt"
toyOneOutput = "toyOneOutput.txt"
toyTwoOutput = "toyTwoOutput.txt"
toyThreeOutput = "toyThreeOutput.txt"
tool_dir = os.path.dirname(__file__)


# FIXME: use tempfile.mkdtemp to create temporary directory for workDir

class ToyWorkflow(WorkFlow):
    def __init__(self, data_dir):
        setUp = Step(
            command="touch {0}/bucket.txt".format(data_dir),
            inputs=set(),
            outputs={"bucket.txt"})
        toyOne = Step(
            command="touch {0}/{toyOneOutput}".format(data_dir, **globals()),
            inputs={"bucket.txt"},
            outputs={"toyOneOutput.txt"})
        toyTwo = Step(
            command="touch {0}/{toyTwoOutput}".format(data_dir, **globals()),
            inputs={"toyOneOutput.txt"},
            outputs={"toyTwoOutput.txt"})
        toyThree = Step(
            command="touch {0}/{toyThreeOutput}".format(data_dir, **globals()),
            inputs={"toyTwoOutput.txt"},
            outputs={"toyThreeOutput.txt"})
        steps = [setUp, toyOne, toyTwo, toyThree]
        self.steps = steps
        WorkFlow.__init__(self, self.steps, data_dir)


class TestWorkFlow(unittest.TestCase):

    def setUp(self):
        self.workdir = tempfile.mkdtemp()

    def test_simple(self):
        workflow = ToyWorkflow(self.workdir)

        index = 0
        step_index = workflow.current_step()
        deletable_files = workflow.deletable_files()
        self.assertEqual(step_index, index)
        self.assertEqual(len(deletable_files), 0)
        command = workflow.steps[index].command
        subprocess.check_call(command, shell=True)
        all_files = workflow.all_files

        index = 1
        step_index = workflow.current_step()
        deletable_files = workflow.deletable_files()
        self.assertEqual(step_index, index)
        self.assertEqual(len(deletable_files), 0)
        command = workflow.steps[index].command
        subprocess.check_call(command, shell=True)
        all_files = workflow.all_files

        index = 2
        step_index = workflow.current_step()
        deletable_files = workflow.deletable_files()
        self.assertEqual(step_index, index)
        self.assertEqual(len(deletable_files), 1)
        command = workflow.steps[index].command
        subprocess.check_call(command, shell=True)
        all_files = workflow.all_files

        index = 3
        step_index = workflow.current_step()
        deletable_files = workflow.deletable_files()
        self.assertEqual(len(deletable_files), 2)
        self.assertEqual(step_index, index)
        command = workflow.steps[index].command
        subprocess.check_call(command, shell=True)
        all_files = workflow.all_files

    def tearDown(self):
        shutil.rmtree(self.workdir)
