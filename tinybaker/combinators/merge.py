from typing import List, Iterable
from ..transform import Transform, TransformMeta
from ..exceptions import BakerError, TagConflictError
from threading import Thread
from queue import Queue
from typeguard import typechecked


class MergeWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            instance, run_info = self.queue.get()
            try:
                instance._exec_with_run_info(run_info)
            finally:
                self.queue.task_done()


@typechecked
def merge(merge_steps: Iterable[TransformMeta]) -> TransformMeta:
    """
    Merge several transformations together. Base transformations must not conflict in output.

    :param merge_steps: Iterable of Transforms to merge together
    :return: Transform class consisting of a merge between
    """
    merge_steps = list(merge_steps)
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

            if self.context.parallel_mode == "multithreading":
                queue = Queue()
                for _ in range(min(len(instances), self.context.max_threads)):
                    worker = MergeWorker(queue)
                    worker.daemon = True
                    worker.start()

                for instance in instances:
                    queue.put((instance, self._current_run_info))
                queue.join()
            else:
                for instance in instances:
                    instance._exec_with_run_info(self._current_run_info)

    return Merged
