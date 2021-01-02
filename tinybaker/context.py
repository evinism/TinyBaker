from fs.memoryfs import MemoryFS
from fs.tempfs import TempFS
from fs.osfs import OSFS
from .exceptions import BakerError
from .workarounds import is_fileset
from .scheduler import (
    SerialScheduler,
    ProcessScheduler,
    ThreadScheduler,
    BaseScheduler,
)
from collections import namedtuple

BakerConfig = namedtuple(
    "BakerConfig",
    ["fs_for_intermediates", "parallel_mode", "max_threads", "max_processes"],
)


class BakerWorkerContext:
    # Intended to be shared between processes.
    def __init__(self, baker_config: BakerConfig, scheduler: BaseScheduler):
        self.open_fses = {}
        self.baker_config = baker_config
        self.scheduler = scheduler

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

    def execute(self, instances):
        if len(instances) == 1:
            # If there's only one item, run it in the current thread.
            SerialScheduler().run_parallel(instances, self)
            return
        self.scheduler.run_parallel(instances, self)

    # This defines what's shared between processes.
    def __reduce__(self):
        return (BakerWorkerContext, (self.baker_config, self.scheduler))


class BakerDriverContext:
    """
    Driver Context for running TinyBaker transforms

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
        # If we're using multiprocessing, we HAVE to use nvtemp filesystem
        # for intermediates. This is until i get better at stuff.
        if parallel_mode == "multiprocessing" and fs_for_intermediates != "nvtemp":
            raise BakerError(
                "Multiprocessing requires fs_for_intermediates of nvtemp (for nonvolatile temp)"
            )
        self.baker_config = BakerConfig(
            fs_for_intermediates, parallel_mode, max_threads, max_processes
        )
        self.scheduler = self._get_scheduler(parallel_mode)

    @staticmethod
    def _get_scheduler(parallel_mode):
        if parallel_mode == "multiprocessing":
            scheduler = ProcessScheduler()
        elif parallel_mode == "multithreading":
            scheduler = ThreadScheduler()
        else:
            scheduler = SerialScheduler()
        return scheduler

    def run(self, transform):
        worker_context = BakerWorkerContext(self.baker_config, self.scheduler)
        with worker_context:
            worker_context.execute([transform])

    def __reduce__(self):
        raise NotImplementedError("Should not serialize and share driver object!")


_default_context = BakerDriverContext()


def get_default_context():
    return _default_context