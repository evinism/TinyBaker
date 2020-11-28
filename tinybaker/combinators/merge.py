from typing import List
from ..step_definition import StepDefinition
from ..exceptions import BakerError, TagConflictError


def merge(merge_steps: List[StepDefinition]):
    merge_input_file_set = set.union(*[step.input_file_set for step in merge_steps])
    merge_output_file_set = set.union(*[step.output_file_set for step in merge_steps])
    if len(merge_output_file_set) != sum(
        [len(step.output_file_set) for step in merge_steps]
    ):
        # TODO: Tell which outputs are conflicting. But I don't wanna do that yet.
        raise TagConflictError("Output conflicts while merging!")

    class Merged(StepDefinition):
        nonlocal merge_steps, merge_input_file_set, merge_output_file_set

        input_file_set = merge_input_file_set
        output_file_set = merge_output_file_set

        steps = merge_steps

        def script(self):
            merge_input_paths = {
                tag: self.input_files[tag].path for tag in self.input_files
            }
            merge_output_paths = {
                tag: self.output_files[tag].path for tag in self.output_files
            }

            instances = []

            for step in self.steps:
                input_files = {
                    tag: merge_input_paths[tag]
                    for tag in merge_input_paths
                    if tag in step.input_file_set
                }
                output_files = {
                    tag: merge_output_paths[tag]
                    for tag in merge_output_paths
                    if tag in step.output_file_set
                }
                instances.append(step(input_files, output_files))

            # This should be made parallel
            for instance in instances:
                instance.build(overwrite=True)

    return Merged
