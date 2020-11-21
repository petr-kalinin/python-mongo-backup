#!/usr/bin/python3
import atexit
import datetime
import graphyte
import os
import pprint
import pymongo
import shutil
import subprocess
import time
import typing

MONGODB_URI = os.environ['MONGODB_URI']
MONGO_DATA_DIR = './data'
MONGO_COMMAND = ['mongod', '--dbpath', MONGO_DATA_DIR]
BACKUPS_DIR = './backups'
MAX_BACKUPS = 7
BACKUP_PERIOD_SEC = 60 * 60 * 24
BACKUP_FRACTION_SEC = 60 * 60 * 2.5
STATS_PERIOD_SEC = 5 * 60

g_mongo_process : typing.Optional[subprocess.Popen] = None

def mongodump_command(uri, file):
    return ['mongodump', '--gzip', '--archive=' + file, '--uri=' + uri]

def mongorestore_command(file):
    return ['mongorestore', '--gzip', '--archive=' + file]

def run_backup(file):
    print("Starting backup to", file)
    subprocess.check_call(mongodump_command(MONGODB_URI, file))

def ensure_mongo_started():
    global g_mongo_process
    if g_mongo_process is None or g_mongo_process.poll():
        print("Starting mongo")
        g_mongo_process = subprocess.Popen(MONGO_COMMAND, stdout=subprocess.DEVNULL)
        time.sleep(5)
    if g_mongo_process is None or g_mongo_process.poll():
        raise Exception("Could not start mongo")

def stop_mongo():
    if g_mongo_process is not None and not g_mongo_process.poll():
        print("Stopping mongo")
        g_mongo_process.kill()
        time.sleep(5)

def restore_backup(file):
    stop_mongo()
    shutil.rmtree(MONGO_DATA_DIR, ignore_errors=True)
    os.makedirs(MONGO_DATA_DIR)
    ensure_mongo_started()
    subprocess.check_call(mongorestore_command(file))

def get_db_size():
    ensure_mongo_started()
    client = pymongo.MongoClient()
    documents = 0
    collections = 0
    databases = 0
    for dbname in client.list_database_names():
        databases += 1
        for collectionname in client[dbname].list_collection_names():
            collections += 1
            documents += client[dbname][collectionname].count_documents({})
    return {
        "documents": documents,
        "collections": collections,
        "databases": databases
    }

def list_backups():
    os.makedirs(BACKUPS_DIR, exist_ok=True)
    files = os.listdir(BACKUPS_DIR)
    files = [BACKUPS_DIR + "/" + fname for fname in files]
    files.sort(key=lambda fname: os.path.getmtime(fname), reverse = True)
    return files

def cleanup_backups():
    files = list_backups()
    outdated_backups = files[MAX_BACKUPS:]
    for file in outdated_backups:
        print("Deleting outdated backup", file)
        os.remove(file)

def get_last_backup_time():
    files = list_backups()
    if not files:
        return 0
    return os.path.getmtime(files[0])

def get_next_backup_time():
    last_time = get_last_backup_time()
    print("Last time is ", last_time)
    return (last_time // BACKUP_PERIOD_SEC + 1) * BACKUP_PERIOD_SEC + BACKUP_FRACTION_SEC

def maybe_run_backup():
    next_time = get_next_backup_time()
    print("Next backup at", next_time, "now it's", time.time())
    if time.time() > next_time:
        fname = BACKUPS_DIR + "/backup_" + datetime.datetime.now().isoformat()
        run_backup(fname)
        restore_backup(fname)
        cleanup_backups()

def get_stats():
    stats = get_db_size()
    backups = list_backups()
    age = time.time() - get_last_backup_time()
    stats["age"] = age
    stats["backups_count"] = len(backups)
    return stats

def send_stats(stats):
    pprint.pprint(stats)
    for key in stats:
        graphyte.send(key, stats[key])

atexit.register(stop_mongo)
graphyte.init('ije.algoprog.ru', prefix='algoprog.0.backup')
while True:
    maybe_run_backup()
    send_stats(get_stats())
    time.sleep(STATS_PERIOD_SEC)

