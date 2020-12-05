from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from .exceptions import BakerError
from .workarounds.annot import is_fileset


class RunInfo:
    def __init__(self):
        self.open_fses = {}

    def __enter__(self):
        self.open_fses = {"mem": MemoryFS(), "temp": TempFS()}

    def __exit__(self, exc_type, exc_val, exc_tb):
        for prefix in self.open_fses:
            fs = self.open_fses[prefix]
            fs.close()


class BakerContext:
    def __init__(self, fs_for_intermediates="temp"):
        self.fs_for_intermediates = fs_for_intermediates

    def run_transform(self, transform):
        run_info = RunInfo()
        with run_info:
            transform._exec_with_run_info(run_info)


_default_context = BakerContext()


def get_default_context():
    return _default_context