from typing import List, Set
from uuid import uuid4
import os
from ..step_definition import StepDefinition
from ..exceptions import BakerError, TagConflictError


def sequence(seq_steps: List[StepDefinition], exposed_intermediates: Set[str] = set()):
    # Perform validation that the sequence makes sense.
    if len(seq_steps) < 1:
        raise BakerError("Cannot sequence fewer than 1 event")
    if len(seq_steps) == 1:
        return seq_steps[0]

    additional_outputs = set()
    additional_inputs = set()

    for i in range(len(seq_steps) - 1):
        first = seq_steps[i]
        second = seq_steps[i + 1]
        # Additional Outputs
        unconsumed_outputs = first.output_file_set - second.input_file_set
        if len(set.intersection(unconsumed_outputs, additional_outputs)):
            items = set.intersection(unconsumed_outputs, additional_outputs)
            raise TagConflictError(
                "Multiple steps in sequence generate output tag {}".format(
                    ", ".join(items)
                )
            )
        # Handle exposed intermediates for this particular item
        # TODO: Handle the issue where multiple steps may expose
        # this intermediate, probably by moving up in the chain
        intermediates_to_expose = set.intersection(
            first.output_file_set, exposed_intermediates
        )
        if len(intermediates_to_expose) > 0:
            additional_outputs = additional_outputs.union(intermediates_to_expose)

        additional_outputs = additional_outputs.union(unconsumed_outputs)

        # Additional Inputs
        unprovided_inputs = second.input_file_set - first.output_file_set
        if len(set.intersection(unconsumed_outputs, additional_outputs)):
            items = set.intersection(unconsumed_outputs, additional_outputs)
            raise TagConflictError(
                "Multiple steps in sequence expect input tag {}".format(
                    ", ".join(items)
                )
            )
        additional_inputs = additional_inputs.union(unprovided_inputs)

    seq_input_file_set = set.union(seq_steps[0].input_file_set, additional_inputs)
    seq_output_file_set = set.union(seq_steps[-1].output_file_set, additional_outputs)

    class Sequence(StepDefinition):
        nonlocal seq_input_file_set, seq_output_file_set, seq_steps
        input_file_set = seq_input_file_set
        output_file_set = seq_output_file_set

        steps = seq_steps

        def _generate_temp_filename(self, sid):
            # TODO: This really should be in step_definition
            folder = "/tmp/tinybaker-{}".format(sid)
            if not os.path.exists(folder):
                os.mkdir(folder)
            return "{}/{}".format(folder, uuid4())

        def script(self):
            sequence_instance_id = uuid4()
            instances = []

            # Phase 1: build instances
            seq_input_paths = {
                tag: self.input_files[tag].path for tag in self.input_files
            }
            seq_output_paths = {
                tag: self.output_files[tag].path for tag in self.output_files
            }

            prev_output_paths = {}
            for step in self.steps:
                # Step input files
                input_paths_from_sequence = {
                    tag: seq_input_paths[tag]
                    for tag in seq_input_paths
                    if tag in step.input_file_set
                }
                input_paths_from_prev = {
                    tag: prev_output_paths[tag]
                    for tag in prev_output_paths
                    if tag in step.input_file_set
                }
                input_paths = {}
                input_paths.update(input_paths_from_sequence)
                input_paths.update(input_paths_from_prev)

                # Generate output fileset
                generated_output_paths = {
                    tag: self._generate_temp_filename(sequence_instance_id)
                    for tag in step.output_file_set - self.output_file_set
                }
                output_paths_from_sequence = {
                    tag: seq_output_paths[tag]
                    for tag in seq_output_paths
                    if tag in step.output_file_set
                }
                output_paths = {}
                output_paths.update(generated_output_paths)
                output_paths.update(output_paths_from_sequence)

                instances.append(
                    step(input_paths=input_paths, output_paths=output_paths)
                )

                # maintain state
                prev_output_paths = output_paths

            # Phase 2: Run instances
            for instance in instances:
                # TODO: Figure out how better to handle overwrites.
                # Right now, this is handled by the parent class.
                instance.build(overwrite=True)

    return Sequence
