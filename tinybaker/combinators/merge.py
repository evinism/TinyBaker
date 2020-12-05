from typing import List
from ..transform import Transform
from ..exceptions import BakerError, TagConflictError
from multiprocessing import Pool
from ..context import ParallelMode


def _exec_standalone(instance, _current_run_info):
    instance._exec_with_run_info(_current_run_info)


def merge(merge_steps: List[Transform]):
    merge_input_tags = set.union(*[step.input_tags for step in merge_steps])
    merge_output_tags = set.union(*[step.output_tags for step in merge_steps])
    if len(merge_output_tags) != sum([len(step.output_tags) for step in merge_steps]):
        # TODO: Tell which outputs are conflicting. But I don't wanna do that yet.
        raise TagConflictError("Output conflicts while merging!")

    class Merged(Transform):
        nonlocal merge_steps, merge_input_tags, merge_output_tags

        input_tags = merge_input_tags
        output_tags = merge_output_tags

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
                    if tag in step.input_tags
                }
                output_files = {
                    tag: merge_output_paths[tag]
                    for tag in merge_output_paths
                    if tag in step.output_tags
                }
                instances.append(
                    step(
                        input_files,
                        output_files,
                        context=self.context,
                        overwrite=self.overwrite,
                    )
                )

            if self.context.parallel_mode == ParallelMode.MULTIPROCESSING:
                # This should be made parallel
                with Pool(processes=len(instances)) as pool:
                    args = [
                        (instance, self._current_run_info) for instance in instances
                    ]
                    pool.starmap(_exec_standalone, args)
            else:
                for instance in instances:
                    _exec_standalone(instance, self._current_run_info)

    return Merged
