from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from .exceptions import BakerError
from .workarounds.annot import is_fileset
from .util import affected_files_for_transform


class RunInfo:
    def __init__(self, transform, parent):
        self.transform = transform
        self.open_fses = {}
        # UGGGLLYYY
        self.parent = parent

    def __enter__(self):
        self.parent.current_runs.append(self)
        self.open_fses = {"mem": MemoryFS(), "temp": TempFS()}

    def __exit__(self, exc_type, exc_val, exc_tb):
        for prefix in self.open_fses:
            fs = self.open_fses[prefix]
            fs.close()
        self.parent.current_runs = [
            run for run in self.parent.current_runs if run != self
        ]


class BakerContext:
    def __init__(self, fs_for_intermediates="temp"):
        self.current_runs = []
        self.fs_for_intermediates = fs_for_intermediates

    def _current_affected_files(self):
        retval = set()
        for run in self.current_runs:
            retval = retval.union(affected_files_for_transform(run.transform))
        return retval

    def run_transform(self, transform):
        file_overlap = set.intersection(
            affected_files_for_transform(transform), self._current_affected_files()
        )
        if len(file_overlap):
            raise BakerError(
                "Files {} already being used by a separate run!".format(
                    ", ".join(file_overlap)
                )
            )
        run_info = RunInfo(transform, self)
        with run_info:
            transform._exec_with_run_info(run_info)

    def construct_run_info(self):
        return {}


_default_context = BakerContext()


def get_default_context():
    return _default_context