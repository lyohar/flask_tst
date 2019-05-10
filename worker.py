from multiprocessing import Process
from random import randint
from time import sleep
from log import logger
import requests
import json

class Worker(Process):

    def __init__(self, salt):
        Process.__init__(self)
        self._salt = salt
        self._sleep_time = randint(2, 6)
        self._get_work_url = "http://localhost:5000/get_work"
        self._set_work_status_url = "http://localhost:5000/set_work_status"
        self._delay_after_empty_task = 3

    def run(self):
        while True:
            resp = requests.get(
                url = self._get_work_url,
                headers = {'salt': self._salt}
            ).json()
            try:
                task_id = resp['task_id']
                delay = resp['delay']
            except Exception as e:
                logger.error(f"the exception is: {e}, while responce is {resp}")
            if (task_id == -1):
                # no work got from scheduler
                sleep(self._delay_after_empty_task)
                continue
            if (task_id == -2):
                logger.info("got terminate command, will exit now")
                break
            logger.info(f"will simulate execution of task {task_id} for {delay} seconds")
            sleep(delay)
            resp = requests.post(
                url = self._set_work_status_url,
                data = json.dumps({'task_id': task_id, 'status': 'SUCCESS'}),
                headers = {'Content-type': 'application/json', 'salt': self._salt}
            ).json()
            logger.info(f"got responce for responce: {resp}")
