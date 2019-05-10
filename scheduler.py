from log import logger
import psycopg2
from worker import Worker
import crypt
from threading import Lock

class Scheduler:

    def __init__(self):
        logger.info("connecting to repo")
        self._repo = psycopg2.connect(
            dbname = "repo",
            user = "postgres",
            password = "postgres",
            host = "localhost",
            port = 5432
        )
        self._cursor = self._repo.cursor()
        logger.info("connected to repo")

        self._workers=[]
        self._remove_workers = 0
        self._lock = Lock()

    def set_work_status(self, task_id, status):
        #self._lock.acquire()
        #try:
            self._cursor.execute(f"update tasks set status='{status}' where task_id={task_id}")
            self._repo.commit()
            logger.info(f"the status for task {task_id} was updated to {status}")
        #finally:
        #    self._lock.release()

    def get_work(self, salt):
        self._lock.acquire()
        try:
            #check if worker is registered
            #TODO refactor
            found = False
            index = 0
            for w in self._workers:
                if w[0] == salt:
                    found = True
                    break
                index += 1
            if not found:
                logger.info(f"worker with salt was not found in worker list")
                raise Exception("salt not found")

            if self._remove_workers > 0:
                logger.info("removing worker")
                del self._workers[index]
                self._remove_workers -= 1
                return -2, -2

            self._cursor.execute("select task_id, delay from tasks where status = 'READY' order by task_id limit 1")
            row = self._cursor.fetchall()
            if (len(row) == 0):
                logger.info("there is no work to perform")
                return -1, -1
            else:
                task_id = row[0][0]
                delay = row[0][1]
                logger.info(f"got task with id {task_id} and delay {delay}")
                self.set_work_status(task_id, "EXECUTING")
                return task_id, delay
        finally:
            self._lock.release()

    def add_worker(self):
        logger.info("will add worker")
        salt = crypt.mksalt(crypt.METHOD_SHA512)
        w = Worker(salt=salt)
        w.start()
        pid = w.pid
        self._workers.append((salt, pid))
        logger.info("worker started")

    def set_remove_worker_flag(self):
        res = False
        if (len(self._workers) - self._remove_workers <= 0):
            logger.info("there are no more workers to remove")
        else:
            logger.info("adding workers to remove")
            self._remove_workers += 1
            res = True
        return res

    def get_workers(self):
        return self._workers

if __name__ == '__main__':
    s = Scheduler()
    logger.info(s.get_work())

