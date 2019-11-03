# Created by AlecZ
# Tested with Python 3.4
#
# These example classes make it easy to multiprocess* in Python, 
#   useful if you want to take advantage of multiple cores on a single machine
#   or run lots of low-CPU-usage but blocking tasks like web scrapers
#   without configuring a more permanent solution like Celery workers.
#
# * Actually runs multiple Python processes to take advantage of more cores, 
#   unlike Python's `multithreading` module helpers that actually don't run things in parallel.
#
# Warning: You can send most types (including custom ones) in the parameters, 
#   but don't send anything that relies on external state, like file handles or psycopg2 cursors.
#   Not sure exactly what happens if you do that, but probably bad things.
#   Also, some things like functions with Java bindings won't work properly in child processes.

from multiprocessing import Process, Queue
from threading import Thread


# more advanced: uses a worker pool and gets results in order, like Celery
class Batcher():

    CMD_JOB = 0
    CMD_KILL = 1

    def __init__(self, num_workers):
        self.jobs = []
        self.num_workers = num_workers

        # spawn workers
        self.in_queue, self.out_queue = Queue(), Queue()
        self.workers = []
        for _ in range(num_workers):
            p = Thread(target=self._worker, args=(self.in_queue, self.out_queue))
            self.workers.append(p)
            p.start()

    def __del__(self):
        # stop workers
        for _ in range(self.num_workers):
            self.in_queue.put((self.CMD_KILL, None, None))
        for i in range(self.num_workers):
            self.workers[i].join()

    @staticmethod
    def _worker(in_queue, out_queue):
        while True:
            # listen for new jobs
            cmd, index, job = in_queue.get()
            if cmd == Batcher.CMD_JOB:
                # process job, return result
                func, args, kwargs = job
                ret = func(*args, **kwargs)
                out_queue.put((index, ret))
            elif cmd == Batcher.CMD_KILL:
                # time to stop
                return
            else:
                assert False

    def enqueue(self, func, *args, **kwargs):
        job = (func, args, kwargs)
        self.jobs.append(job)

    def process(self):
        # put jobs into queue
        job_idx = 0
        for start in range(0, len(self.jobs), self.num_workers):
            for job in self.jobs[start: start + self.num_workers]:
                self.in_queue.put((Batcher.CMD_JOB, job_idx, job))
                job_idx += 1

        # get results from workers
        results = [None] * len(self.jobs)
        for _ in range(len(self.jobs)):
            res_idx, res = self.out_queue.get()
            assert results[res_idx] == None
            results[res_idx] = res

        self.jobs = []
        return results


# tester/examples
if __name__ == "__main__":
    for _ in range(4):
        mp2 = Batcher(num_workers=4)
        num_jobs = 64
        # same, but this time we sum a different set of numbers each time and care about the results' order
        for i in range(num_jobs):
            mp2.enqueue(sum, [1, 2, 3, 4, 5, i])
        ret = mp2.process()
        print(ret)
        assert len(ret) == num_jobs and all(r == 15 + i for i, r in enumerate(ret))
        del mp2
