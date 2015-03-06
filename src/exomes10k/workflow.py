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
        # set(s3) ensures that a new set is created, a defensive copy
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
            # If the step has all the necessary inputs, and has not produced the necessary outputs, we return it
            if step.inputs.issubset(self.existing_output) and not step.outputs.issubset(self.existing_output):
                return index
        return 0

    def get_dir(self):
        return self.work_dir

    def deletable_files(self):
        """
        Returns the output files that can be deleted (aren't used again in the workflow)
        """
        index = self.current_step()
        cannot_be_deleted = set()
        for file in self.existing_output:
            for step in self.steps[index:]:
                if file in step.inputs:
                    cannot_be_deleted.add(file)
        return self.existing_output - cannot_be_deleted

    # property tag treats function like variable
    @property
    def existing_output(self):
        """
        Returns the existing output files from the current directory
        """
        files = [f for f in os.listdir(self.work_dir) if os.path.isfile(os.path.join(self.work_dir, f))]
        output_set = set()
        for f in files:
            for step in self.steps:
                if f in step.outputs:
                    output_set.add(f)
        return output_set
