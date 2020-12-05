from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from .exceptions import BakerError
from .workarounds.annot import is_fileset
from enum import Enum
import warnings


class ParallelMode(Enum):
    MULTIPROCESSING = "multiprocessing"


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
    def __init__(self, fs_for_intermediates="temp", parallel_mode=None):
        self.fs_for_intermediates = fs_for_intermediates
        if parallel_mode:
            warnings.warn(
                "Parallel mode is barely supported and is highly experimental! Expect errors!"
            )
        self.parallel_mode = parallel_mode

    def run_transform(self, transform):
        run_info = RunInfo()
        with run_info:
            transform._exec_with_run_info(run_info)


_default_context = BakerContext()


def get_default_context():
    return _default_context