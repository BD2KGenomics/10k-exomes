import os


class Step:
    inputs = set()
    outputs = set()

    def __init__(self, command, inputs, outputs, function=None,):
        self.command = command
        self.outputs = outputs
        self.inputs = inputs
        if(function is None):
            self.function = self.blank

    @staticmethod
    def blank(self):
        pass

class WorkFlow:
    def __init__(self, steps, work_dir, s3_files=None):
        """
        """
        self.steps = steps
        self.work_dir = work_dir
        # set(s3) ensures that a new set is created, a defensive copy
        self.s3_files = set() if s3_files is None else set(s3_files)

    def current_step(self):
        """
        Returns the index of the workflow step that needs to be run next
        """
        #
        for index, step in enumerate(reversed(self.steps)):
            # If the step has all the necessary inputs, and has not produced the necessary outputs, we return it
            existing_files = self.existing_output
            if step.inputs.issubset(existing_files) and not step.outputs.issubset(existing_files):
                return len(self.steps) - index - 1  # this returns equivelent index in the non reversed step list
            else:
                if step.outputs.issubset(existing_files) and index+1 == len(self.steps):
                    #FIXME we should actually terminate program here since all the work has been done.
                    print("Workflow Complete")
                    return -1
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

    def delete_locally(self, deletable):
        """
        given set of deletable files this deletes them from the local system
        """
        for file in list(deletable):
            os.remove(self.work_dir+"/"+file)

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
