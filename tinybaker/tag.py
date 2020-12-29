import contextvars
from .workarounds.annot import get_annotation
from .exceptions import BakerError

input_files_ctx = contextvars.ContextVar("input_files")
output_files_ctx = contextvars.ContextVar("output_files")


# Bad workaround for annotations for tags not allowing easy subclassing here...
def only_for_annotation(annot):
    def inner(fn):
        def wrapped(self, *args, **kwargs):
            if get_annotation(self.name) != annot:
                raise BakerError(
                    "Function {} not implemented for tag annotation {}".format(
                        fn.__name__, annot
                    )
                )
            return fn(self, *args, **kwargs)

        return wrapped

    return inner


class BaseTag:
    FILE = "file"
    FILESET = "fileset"

    def __init__(self, name, in_or_out):
        self.name = name
        self.in_or_out = in_or_out

    # Ugh tags should inherit from filerefs or from lists respectively.
    @only_for_annotation("file")
    def open(self, *args, **kwargs):
        return self.ref.open(*args, **kwargs)

    @only_for_annotation("file")
    def openbin(self, *args, **kwargs):
        return self.ref.openbin(*args, **kwargs)

    @only_for_annotation("file")
    def exists(self, *args, **kwargs):
        return self.ref.exists(*args, **kwargs)

    @only_for_annotation("file")
    def touch(self):
        self.ref.touch()

    @property
    @only_for_annotation("file")
    def path(self):
        return self.ref.path

    @only_for_annotation("fileset")
    def __iter__(self, *args, **kwargs):
        return self.ref.__iter__(*args, **kwargs)

    @property
    def ref(self):
        raise BakerError("Cannot reference target outside of a TinyBaker context!")


class InputTag(BaseTag):
    def __init__(self, name):
        super().__init__(name, "in")

    @property
    def ref(self):
        return input_files_ctx.get()[self.name]


class OutputTag(BaseTag):
    def __init__(self, name):
        super().__init__(name, "out")

    @property
    def ref(self):
        return output_files_ctx.get()[self.name]
