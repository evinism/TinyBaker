import uuid
from .scheduler import (
    SerialScheduler,
    ProcessScheduler,
    ThreadScheduler,
    BaseScheduler,
)
from collections import namedtuple

from fsspec.implementations.local import LocalFileSystem

BakerConfig = namedtuple(
    "BakerConfig",
    ["fs_for_intermediates", "parallel_mode", "max_threads", "max_processes"],
)


class BakerWorkerContext:
    # Intended to be shared between processes.
    def __init__(
        self, baker_config: BakerConfig, scheduler: BaseScheduler, run_id=None
    ):
        self.baker_config = baker_config
        self.scheduler = scheduler
        self.run_id = run_id
        if self.run_id is None:
            self.run_id = uuid.uuid4().hex
        self.tmp_path = f"/tmp/tinybaker/run-{self.run_id}"

    def __enter__(self):
        pass

    def __exit__(self, *_):
        # cleanup!
        fs = LocalFileSystem()
        if fs.exists(self.tmp_path):
            fs.rm(self.tmp_path, recursive=True)

    def execute(self, instances):
        if len(instances) == 1:
            # If there's only one item, run it in the current thread.
            SerialScheduler().run_parallel(instances, self)
            return
        self.scheduler.run_parallel(instances, self)

    # This defines what's shared between processes.
    def __reduce__(self):
        return (BakerWorkerContext, (self.baker_config, self.scheduler, self.run_id))


class BakerDriverContext:
    """
    Driver Context for running TinyBaker transforms

    :param optional fs_for_intermediates:
        Which filesystem to use to store intermediates. You probably want this to be "file" or "memory"
    :param optional max_threads: The max number of threads that TinyBaker can spawn.
    :param optional parallel_mode:
        What parallelism mode to run TinyBaker in. Options are None and "multithreading". These will
        probably expand over time. Experimental "multiprocessing" value can also be used.
    """

    def __init__(
        self,
        fs_for_intermediates="file",
        max_threads=8,
        max_processes=8,
        parallel_mode="multithreading",
    ):
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