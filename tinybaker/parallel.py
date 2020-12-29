from threading import Thread
from multiprocessing import Pool
from queue import Queue


def _mp_run(arg):
    instance, current_run_info = arg
    return instance._exec_with_run_info(current_run_info)


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


def run_parallel(instances, context, current_run_info):
    if context.parallel_mode == "multithreading":
        queue = Queue()
        for _ in range(min(len(instances), context.max_threads)):
            worker = ParallelWorker(queue)
            worker.daemon = True
            worker.start()

        for instance in instances:
            queue.put((instance, current_run_info))
        queue.join()
    elif context.parallel_mode == "multiprocessing":
        with Pool(min(len(instances), context.max_processes)) as p:
            mp_args = [(instance, current_run_info) for instance in instances]
            p.map(_mp_run, mp_args)
    else:
        for instance in instances:
            instance._exec_with_run_info(current_run_info)