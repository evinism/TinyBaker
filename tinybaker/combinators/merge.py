from typing import List, Iterable, Any
from ..transform import Transform, TransformMeta, coerce_to_transform
from ..exceptions import BakerError, TagConflictError
from ..util import classproperty
from typeguard import typechecked
from .base import CombinatorBase


@typechecked
def merge(merge_steps: Iterable[Any], name: str = None) -> TransformMeta:
    """
    Merge several transformations together. Base transformations must not conflict in output.

    :param merge_steps: Iterable of Transforms to merge together
    :param optional name: The name of the resulting transform
    :return: Transform class consisting of a merge between the input transforms
    """
    merge_steps = [coerce_to_transform(step) for step in merge_steps]
    merge_input_tags = set.union(*[step.input_tags for step in merge_steps])
    merge_output_tags = set.union(*[step.output_tags for step in merge_steps])
    if len(merge_output_tags) != sum([len(step.output_tags) for step in merge_steps]):
        # TODO: Tell which outputs are conflicting. But I don't wanna do that yet.
        raise TagConflictError("Output conflicts while merging!")

    merge_name = name
    return _create_merge_class(
        merge_steps, merge_input_tags, merge_output_tags, merge_name
    )


def _create_merge_class(merge_steps, merge_input_tags, merge_output_tags, merge_name):
    class Merged(CombinatorBase):
        nonlocal merge_steps, merge_input_tags, merge_output_tags, merge_name
        __creation_values__ = (
            _create_merge_class,
            merge_steps,
            merge_input_tags,
            merge_output_tags,
            merge_name,
        )

        input_tags = merge_input_tags
        output_tags = merge_output_tags

        parallelism = len(merge_steps)

        substeps = merge_steps
        _name = merge_name

        @classmethod
        def structure(cls):
            struct = super(Merged, cls).structure()
            struct["type"] = "merge"
            struct["steps"] = [step.structure() for step in cls.substeps]
            return struct

        @classproperty
        def name(cls):
            if cls._name:
                return cls._name
            return "Merge({})".format(",".join([step.name for step in cls.substeps]))

        def script(self):
            merge_input_paths = {
                tag: self.input_files[tag].path for tag in self.input_files
            }
            merge_output_paths = {
                tag: self.output_files[tag].path for tag in self.output_files
            }

            instances = []

            for step in self.substeps:
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
                        overwrite=self.overwrite,
                    )
                )
            self._current_worker_context.execute(instances)

    return Merged
