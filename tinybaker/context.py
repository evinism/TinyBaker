from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from fs.osfs import OSFS
from .exceptions import BakerError
from .workarounds import is_fileset
from .parallel import run_parallel


class RunInfo:
    def __init__(self):
        self.open_fses = {}

    def __enter__(self):
        self.open_fses = {
            "mem": MemoryFS(),
            "temp": TempFS(),
            "nvtemp": OSFS("/tmp/tinybaker-nv-temp", create=True),
        }

    def __exit__(self, exc_type, exc_val, exc_tb):
        for prefix in self.open_fses:
            fs = self.open_fses[prefix]
            fs.close()

    # This defines what's shared between processes. Right now, the
    # answer is "nothing", which we can get away with due to requiring
    # nvtemp for multiprocessing.
    def __reduce__(self):
        return (RunInfo, ())


class BakerContext:
    """
    Execution Context for running TinyBaker transforms

    :param optional fs_for_intermediates:
        Which filesystem to use to store intermediates. You probably want this to be "temp" or "mem"
    :param optional max_threads: The max number of threads that TinyBaker can spawn.
    :param optional parallel_mode:
        What parallelism mode to run TinyBaker in. Options are None and "multithreading". These will
        probably expand over time. Experimental "multiprocessing" value can also be used.
    """

    def __init__(
        self,
        fs_for_intermediates="temp",
        max_threads=8,
        max_processes=8,
        parallel_mode="multithreading",
    ):
        self.fs_for_intermediates = fs_for_intermediates
        self.max_threads = max_threads
        self.max_processes = max_processes
        self.parallel_mode = parallel_mode

        # If we're using multiprocessing, we HAVE to use nvtemp filesystem
        # for intermediates. This is until i get better at stuff.
        if parallel_mode == "multiprocessing" and fs_for_intermediates != "nvtemp":
            raise BakerError(
                "Multiprocessing requires fs_for_intermediates of nvtemp (for nonvolatile temp)"
            )

    def run_transform(self, transform):
        run_info = RunInfo()
        with run_info:
            transform._exec_with_run_info(run_info)

    def run_parallel(self, instances, run_info):
        return run_parallel(instances, self, run_info)


_default_context = BakerContext()


def get_default_context():
    return _default_context