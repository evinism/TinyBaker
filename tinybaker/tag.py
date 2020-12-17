import contextvars

input_files_ctx = contextvars.ContextVar("input_files")
output_files_ctx = contextvars.ContextVar("output_files")


class BaseTag:
    FILE = "file"
    FILESET = "fileset"

    def __init__(self, name, in_or_out):
        self.name = name
        self.in_or_out = in_or_out

    @property
    def target(self):
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
