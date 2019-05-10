from flask import Flask, jsonify, request
import datetime
import scheduler
from log import logger
from gevent.pywsgi import WSGIServer

app = Flask("ods2")

class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route("/get_work")
def get_work():
    salt = request.headers.get('salt')
    if salt is None:
        raise InvalidUsage("salt is required in the header of get_work method")
    try:
        task_id, delay = sched.get_work(salt)
    except Exception as e:
        raise  InvalidUsage(str(e))

    return jsonify(
        task_id = task_id,
        delay = delay
    )

@app.route("/set_work_status", methods=['POST'])
def set_work_status():
    salt = request.headers.get('salt')
    if salt is None:
        raise InvalidUsage("salt is required in the header of set_work_status method")
    req = request.json
    task_id = req['task_id']
    status = req['status']
    logger.info(f"going ro set status {status} for task {task_id}")
    sched.set_work_status(task_id, status)
    return jsonify(
        status = 'OK'
    )

@app.route("/add_worker", methods=['POST'])
def add_worker():
    logger.info(f"adding worker")
    sched.add_worker()
    return jsonify(
        status='OK'
    )

@app.route("/remove_worker", methods=['POST'])
def remove_worker():
    logger.info(f"removing worker")
    res = sched.set_remove_worker_flag()
    if res:
        return  jsonify(
            status = 'OK'
        )
    else:
        raise InvalidUsage("there are no workers to remove")

@app.route("/get_workers")
def get_workers():
    res = []
    workers = sched.get_workers()
    l = len(workers)

    for w in workers:
        res.append({"salt": w[0], "pid": w[1]})
    return jsonify(
        count = l,
        workers = res
    )

if __name__ == '__main__':
    sched = scheduler.Scheduler()
    WSGIServer(('', 5000), app).serve_forever()
    #app.run(host = 'localhost', port=5000)
