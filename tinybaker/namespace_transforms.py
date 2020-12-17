from typing import Dict, Any
import contextvars
from .fileref import FileRef
from .exceptions import BakerError
from .tag import InputTag, OutputTag


def namespace_to_transform(BaseClass, source_ns):
    return dict_to_transform(BaseClass, source_ns.__dict__, source_ns.__name__)


def dict_to_transform(BaseClass, ns_dict: Dict[str, Any], name_outer=None):
    if "script" not in ns_dict:
        raise BakerError(
            "Namespace is missing script export! Is it a TinyBaker transform?"
        )
    if not callable(ns_dict["script"]):
        raise BakerError(
            "Namespace's script export is not callable! Is it a TinyBaker transform?"
        )

    input_tags_outer = set()
    output_tags_outer = set()

    for key in ns_dict:
        value = ns_dict[key]
        if isinstance(value, InputTag):
            input_tags_outer.add(value.name)
        elif isinstance(value, OutputTag):
            output_tags_outer.add(value.name)

    class DerivedTransform(BaseClass):
        nonlocal ns_dict, input_tags_outer, output_tags_outer, name_outer
        _ns_dict = ns_dict
        name = name_outer or "DerivedTransform"

        input_tags = input_tags_outer
        output_tags = output_tags_outer

        def script(self):
            cls = self.__class__
            cls._ns_dict["script"]()

    return DerivedTransform
