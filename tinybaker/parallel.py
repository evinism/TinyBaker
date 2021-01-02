from abc import ABC, abstractmethod
from threading import Thread
from multiprocessing import Pool
from queue import Queue


class BaseParallelizer(ABC):
    @abstractmethod
    def run_parallel(self, instances, current_worker_context):
        pass


class ThreadParallelizer(BaseParallelizer):
    class ParallelWorker(Thread):
        def __init__(self, queue):
            Thread.__init__(self)
            self.queue = queue

        def run(self):
            while True:
                instance, worker_context = self.queue.get()
                try:
                    instance._exec_with_worker_context(worker_context)
                finally:
                    self.queue.task_done()

    def run_parallel(self, instances, current_worker_context):
        queue = Queue()
        parallelism = min(
            len(instances), current_worker_context.baker_config.max_threads
        )
        for _ in range(parallelism):
            worker = ThreadParallelizer.ParallelWorker(queue)
            worker.daemon = True
            worker.start()

        for instance in instances:
            queue.put((instance, current_worker_context))
        queue.join()


class ProcessParallelizer(BaseParallelizer):
    @staticmethod
    def _mp_run(arg):
        instance, current_worker_context = arg
        return instance._exec_with_worker_context(current_worker_context)

    def run_parallel(self, instances, current_worker_context):
        parallelism = min(
            len(instances), current_worker_context.baker_config.max_processes
        )
        with Pool(parallelism) as pool:
            mp_args = [(instance, current_worker_context) for instance in instances]
            pool.map(self._mp_run, mp_args)


class NonParallelizer(BaseParallelizer):
    def run_parallel(self, instances, current_worker_context):
        for instance in instances:
            instance._exec_with_worker_context(current_worker_context)