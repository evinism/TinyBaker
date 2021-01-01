from abc import ABC, abstractmethod
from threading import Thread
from multiprocessing import Pool
from queue import Queue


class ParalellizerBase(ABC):
    @abstractmethod
    def run_parallel(self, context, instances, current_run_info):
        pass

    def __reduce__(self):
        raise NotImplementedError("We really shouldn't be pickling parallelizers")


class ThreadParallelizer(ParalellizerBase):
    class ParallelWorker(Thread):
        def __init__(self, queue):
            Thread.__init__(self)
            self.queue = queue

        def run(self):
            while True:
                instance, run_info = self.queue.get()
                try:
                    instance._exec_with_run_info(run_info)
                finally:
                    self.queue.task_done()

    def run_parallel(self, context, instances, current_run_info):
        queue = Queue()
        for _ in range(min(len(instances), context.max_threads)):
            worker = ThreadParallelizer.ParallelWorker(queue)
            worker.daemon = True
            worker.start()

        for instance in instances:
            queue.put((instance, current_run_info))
        queue.join()


class ProcessParallelizer(ParalellizerBase):
    @staticmethod
    def _mp_run(arg):
        instance, current_run_info = arg
        return instance._exec_with_run_info(current_run_info)

    def run_parallel(self, context, instances, current_run_info):
        with Pool(min(len(instances), context.max_processes)) as p:
            mp_args = [(instance, current_run_info) for instance in instances]
            p.map(self._mp_run, mp_args)


class NonParallelizer(ParalellizerBase):
    def run_parallel(self, context, instances, current_run_info):
        for instance in instances:
            instance._exec_with_run_info(current_run_info)