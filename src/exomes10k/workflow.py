import os


class Step:
    inputs = set()
    outputs = set()

    def __init__(self, command, inputs, outputs):
        self.command = command
        self.outputs = outputs
        self.inputs = inputs


class WorkFlow:
    def __init__(self, steps, work_dir, s3_files=None):
        """
        """
        self.steps = steps
        self.work_dir = work_dir
        self.s3_files = set() if s3_files is None else set(s3_files)

    # normalDownload = Step( command="aws s3 --bucket {bucket} --key {normalKey.name}",
    # input=[],
    # output=[])
    # tumorDownload = Step(command="aws s3 --bucket {bucket} --key {tumorKey.name}", None, None, None)



    def current_step(self):
        """
        Returns the index of the workflow step that needs to be run next
        """
        # FIXME: This loops around (it return 0 when called after the last step was successful
        for index, step in enumerate(self.steps):
            # TODO: explain what the condition does
            if step.inputs.issubset(self.existing_output) and not step.outputs.issubset(self.existing_output):
                return index
        return 0


    def deletable(self):
        index = self.current_step()
        cannot_be_deleted = set()
        for file in self.existing_output:
            for step in self.steps[index:]:
                if file in step.inputs:
                    cannot_be_deleted.add(file)
        return self.existing_output - cannot_be_deleted

    @property
    def existing_output(self):
        files = [f for f in os.listdir(self.work_dir) if os.path.isfile(f)]
        output_set = set()
        for f in files:
            for step in self.steps:
                if f in step.outputs:
                    output_set.add(f)
        return output_set
