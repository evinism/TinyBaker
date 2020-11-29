from typing import List, Set, Dict, NewType, Tuple
from uuid import uuid4
import os
from ..transform import Transform
from ..exceptions import BakerError, TagConflictError


def _build_scope_diagram(steps):
    RefCount = NewType("RefCount", int)
    # Origin step index (-1 for external, default)
    SourceIdx = NewType("SourceIdx", int)
    TagRef = Tuple[SourceIdx, RefCount]
    scope_diagram: Dict[str, TagRef] = {}

    def inc_ref_count(ref: TagRef):
        return (ref[0], ref[1] + 1)

    for i in range(len(steps)):
        step = steps[i]

        for input_tag in step.input_tags:
            # Unreferenced varaible, add it as an external
            if input_tag not in scope_diagram:
                scope_diagram[input_tag] = (-1, 1)
            else:
                scope_diagram[input_tag] = inc_ref_count(scope_diagram[input_tag])

        # append outputs of step i to lexical scope
        for output_tag in step.output_tags:
            if output_tag in scope_diagram:
                raise BakerError(
                    "Multiple steps in sequence generate output tag {}".format(
                        output_tag
                    )
                )
            scope_diagram[output_tag] = (i, 0)
    return scope_diagram


def _determine_sequence_interface(scope_diagram, exposed_intermediates):
    seq_input_tags = {tag for tag in scope_diagram if scope_diagram[tag][0] == -1}
    seq_output_tags = {tag for tag in scope_diagram if scope_diagram[tag][1] == 0}

    non_inputs = set(scope_diagram) - seq_input_tags
    if len(exposed_intermediates - non_inputs):
        items = ", ".join(exposed_intermediates)
        raise BakerError(
            "Attempting to expose non-generated intermediate(s): {}".format(items)
        )
    seq_output_tags = set.union(seq_output_tags, exposed_intermediates)

    return [seq_input_tags, seq_output_tags]


def sequence(seq_steps: List[Transform], exposed_intermediates: Set[str] = set()):
    # Perform validation that the sequence makes sense.
    if len(seq_steps) < 1:
        raise BakerError("Cannot sequence fewer than 1 event")
    if len(seq_steps) == 1:
        return seq_steps[0]

    scope_diagram = _build_scope_diagram(seq_steps)
    seq_input_tags, seq_output_tags = _determine_sequence_interface(
        scope_diagram, exposed_intermediates
    )

    class Sequence(Transform):
        nonlocal seq_input_tags, seq_output_tags, seq_steps
        input_tags = seq_input_tags
        output_tags = seq_output_tags

        steps = seq_steps

        def _generate_temp_filename(self, sid):
            # TODO: This really should be in transform
            folder = "/tmp/tinybaker-{}".format(sid)
            if not os.path.exists(folder):
                os.mkdir(folder)
            return "{}/{}".format(folder, uuid4())

        def script(self, runtime):
            sequence_instance_id = uuid4()
            instances = []

            # Phase 1: build instances
            seq_input_paths = {
                tag: self.input_files[tag].path for tag in self.input_files
            }
            seq_output_paths = {
                tag: self.output_files[tag].path for tag in self.output_files
            }

            lexical_scope = {}
            for step in self.steps:
                # Step input files
                input_paths_from_sequence = {
                    tag: seq_input_paths[tag]
                    for tag in seq_input_paths
                    if tag in step.input_tags
                }
                input_paths_from_scope = {
                    tag: lexical_scope[tag]
                    for tag in lexical_scope
                    if tag in step.input_tags
                }
                input_paths = {}
                input_paths.update(input_paths_from_sequence)
                input_paths.update(input_paths_from_scope)

                # Generate output fileset
                generated_output_paths = {
                    tag: self._generate_temp_filename(sequence_instance_id)
                    for tag in step.output_tags - self.output_tags
                }
                output_paths_from_sequence = {
                    tag: seq_output_paths[tag]
                    for tag in seq_output_paths
                    if tag in step.output_tags
                }
                output_paths = {}
                output_paths.update(generated_output_paths)
                output_paths.update(output_paths_from_sequence)

                instances.append(
                    step(input_paths=input_paths, output_paths=output_paths)
                )

                # maintain state
                lexical_scope.update(output_paths)

            # Phase 2: Run instances
            for instance in instances:
                instance.build(runtime)

    return Sequence
