from typing import Set, Dict, Any
from ..exceptions import BakerError
from ..transform import Transform, TransformMeta, coerce_to_transform
from ..fileref import FileRef
from ..util import classproperty
from typeguard import typechecked


def _map_names(name_set: Set[str], mapping: Dict[str, str]):
    result = set()
    for name in name_set:
        if name in mapping:
            result.add(mapping[name])
        else:
            result.add(name)
    return result


def _map_filerefs_to_new_paths(file_dict: Dict[str, FileRef], mapping: Dict[str, str]):
    result = {}
    for name in file_dict:
        if name in mapping:
            result[mapping[name]] = file_dict[name].path
        else:
            result[name] = file_dict[name].path
    return result


def _invert_mapping(mapping: Dict[str, str]):
    result = {}
    for key in mapping:
        result[mapping[key]] = key
    return result


@typechecked
def map_tags(
    base_step: Any,
    input_mapping: Dict[str, str] = {},
    output_mapping: Dict[str, str] = {},
    name: str = None,
) -> TransformMeta:
    """
    Take a transform and create a new, identical transform with the tags renamed.

    :param base_step: Base step for the transform.
    :param optional input_mapping: Mapping of old input tag names to new input tag names
    :param optional output_mapping: Mapping of old output tag names to new input tag names
    :param optional name: The name of the resulting transform
    :return: Transform class with renamed inputs / outputs
    """
    base_step = coerce_to_transform(base_step)

    extra_input_keys = set(input_mapping) - base_step.input_tags
    if len(extra_input_keys) > 0:
        msg = "Unexpected key(s) for input mapping: {}".format(
            ", ".join(extra_input_keys)
        )
        raise BakerError(msg)

    extra_output_keys = set(output_mapping) - base_step.output_tags
    if len(extra_output_keys) > 0:
        msg = "Unexpected key(s) for output mapping: {}".format(
            ", ".join(extra_output_keys)
        )
        raise BakerError(msg)

    mapping_input_tags = _map_names(base_step.input_tags, input_mapping)
    mapping_output_tags = _map_names(base_step.output_tags, output_mapping)

    nonlocal_name = name

    class TagMapping(Transform):
        nonlocal mapping_input_tags, mapping_output_tags, base_step, input_mapping, output_mapping, nonlocal_name
        input_tags = mapping_input_tags
        output_tags = mapping_output_tags
        _name = nonlocal_name

        _base_step = base_step
        _input_mapping = input_mapping
        _output_mapping = output_mapping

        @classmethod
        def structure(cls):
            struct = super(TagMapping, cls).structure()
            struct["type"] = "map"
            struct["base_step"] = cls._base_step.structure()
            return struct

        @classproperty
        def name(self):
            if self._name:
                return self._name
            return base_step.name

        def script(self):
            input_paths = _map_filerefs_to_new_paths(
                self.input_files, _invert_mapping(self._input_mapping)
            )
            output_paths = _map_filerefs_to_new_paths(
                self.output_files, _invert_mapping(self._output_mapping)
            )

            self._base_step(
                input_paths=input_paths,
                output_paths=output_paths,
                context=self.context,
                overwrite=self.overwrite,
            )._exec_with_run_info(self._current_run_info)

    return TagMapping
