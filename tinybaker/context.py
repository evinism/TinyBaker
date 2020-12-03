from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from .exceptions import BakerError


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


def _affected_files_for_transform(transform):
    retval = set()
    for tag in transform.input_paths:
        retval.add(transform.input_paths[tag])
    for tag in transform.output_paths:
        retval.add(transform.output_paths[tag])
    return retval


class BakerContext:
    def __init__(self, overwrite=False):
        self.overwrite = overwrite
        self.current_runs = []

    def _current_affected_files(self):
        retval = set()
        for run in self.current_runs:
            retval = retval.union(_affected_files_for_transform(run.transform))
        return retval

    def run_transform(self, transform):
        print("Starting run!")
        print("New no. of runs: {}".format(len(self.current_runs) + 1))

        file_overlap = set.intersection(
            _affected_files_for_transform(transform), self._current_affected_files()
        )
        if len(file_overlap):
            raise BakerError(
                "Files {} already being used by a separate run!".format(
                    ", ".join(file_overlap)
                )
            )
        run_info = RunInfo(transform, self)
        with run_info:
            transform.exec_internal(run_info)

    def construct_run_info(self):
        return {}


class DefaultContext(BakerContext):
    pass
