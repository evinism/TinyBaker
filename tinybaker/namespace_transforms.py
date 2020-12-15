import contextvars
from .fileref import FileRef
from .transform import Transform
from .exceptions import BakerError

_input_files_ctx = contextvars.ContextVar("input_files")
_output_files_ctx = contextvars.ContextVar("output_files")


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
        global _input_files_ctx, _output_files_ctx
        ns = source_ns
        name = source_ns.__name__
        input_tag_ctx_ref = _input_files_ctx
        output_tag_ctx_ref = _output_files_ctx

        input_tags = input_tags_outer
        output_tags = output_tags_outer

        def script(self):
            cls = self.__class__
            input_token = cls.input_tag_ctx_ref.set(self.input_files)
            output_token = cls.output_tag_ctx_ref.set(self.output_files)
            cls.ns.script()
            cls.input_tag_ctx_ref.reset(input_token)
            cls.output_tag_ctx_ref.reset(output_token)

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


class InputTag(BaseTag):
    def __init__(self, name, annot=BaseTag.FILE):
        super().__init__(name, annot, "in")

    @property
    def ref(self):
        return _input_files_ctx.get()[self.name]


class OutputTag(BaseTag):
    def __init__(self, name, annot=BaseTag.FILE):
        super().__init__(name, annot, "out")

    @property
    def ref(self):
        return _output_files_ctx.get()[self.name]


# To help combat against circular imports, also python 3.6 support.
setattr(Transform, "from_namespace", staticmethod(namespace_to_transform))
