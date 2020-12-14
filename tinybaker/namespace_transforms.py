import contextvars
from .fileref import FileRef
from .transform import Transform

input_files_ctx = contextvars.ContextVar("input_files")
output_files_ctx = contextvars.ContextVar("input_files")


def namespace_to_transform(source_ns):
    if not hasattr(source_ns, "script"):
        raise BakerError(
            "Namespace is missing script export! Is it a TinyBaker transform?"
        )
    if not callable(source_ns.script):
        raise BakerError(
            "Namespace's script export is not callable! Is it a TinyBaker transform?"
        )

    input_tags_outer = set()
    output_tags_outer = set()

    for key in source_ns.__dict__:
        value = source_ns.__dict__[key]
        if isinstance(value, InputTag):
            input_tags_outer.add(value.name)
        elif isinstance(value, OutputTag):
            output_tags_outer.add(value.name)

    class NamespacedTransform(Transform):
        nonlocal source_ns, input_tags_outer, output_tags_outer
        ns = source_ns
        name = source_ns.__name__

        input_tags = input_tags_outer
        output_tags = output_tags_outer

        @classmethod
        def script(cls):
            cls.ns.script()

    return NamespacedTransform


class BaseTag:
    FILE = "file"
    FILESET = "fileset"

    def __init__(self, name, annot, in_or_out):
        if annot == BaseTag.FILESET:
            name = "fileset::{}".name
        self.name = name
        self.in_or_out = in_or_out

    @property
    def target(self):
        raise BakerError("Cannot reference target outside of a TinyBaker context!")

    # This will get filled in or monkeypatched or something....
    def _activate(fileref: FileRef):
        self.target = fileref


class InputTag(BaseTag):
    def __init__(self, name, annot=BaseTag.FILE):
        super().__init__(name, annot, "in")


class OutputTag(BaseTag):
    def __init__(self, name, annot=BaseTag.FILE):
        super().__init__(name, annot, "out")
