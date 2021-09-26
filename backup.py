#!/usr/bin/python3 -u
import atexit
import datetime
import graphyte
import os
import logging
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
BACKUP_FRACTION_SEC = 60 * 60 * 1.5
STATS_PERIOD_SEC = 3 * 60

# for test
#BACKUP_PERIOD_SEC = 60 * 10
#BACKUP_FRACTION_SEC = 60 * 1

logging.basicConfig(format='%(asctime)s:%(filename)s:%(lineno)d: %(message)s', level=logging.DEBUG)

g_mongo_process = None

def mongodump_command(uri, file):
    return ['mongodump', '--gzip', '--archive=' + file, '--uri=' + uri]

def mongorestore_command(file):
    return ['mongorestore', '--gzip', '--archive=' + file]

def run_backup(file):
    logging.info("Starting backup to " + file)
    subprocess.check_call(mongodump_command(MONGODB_URI, file))

def ensure_mongo_started():
    global g_mongo_process
    if g_mongo_process is None or g_mongo_process.poll():
        logging.info("Starting mongo")
        g_mongo_process = subprocess.Popen(MONGO_COMMAND, stdout=subprocess.DEVNULL)
        time.sleep(5)
    if g_mongo_process is None or g_mongo_process.poll():
        raise Exception("Could not start mongo")

def stop_mongo():
    if g_mongo_process is not None and not g_mongo_process.poll():
        logging.info("Stopping mongo")
        g_mongo_process.kill()
        time.sleep(5)

def restore_backup(file):
    stop_mongo()
    shutil.rmtree(MONGO_DATA_DIR, ignore_errors=True)
    os.makedirs(MONGO_DATA_DIR)
    ensure_mongo_started()
    try:
        subprocess.check_call(mongorestore_command(file))
    except:
        stop_mongo()
        shutil.rmtree(MONGO_DATA_DIR, ignore_errors=True)
        raise
        

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
            this_documents = client[dbname][collectionname].count_documents({})
            print("{}.{} documents = {}".format(dbname, collectionname, this_documents))
            documents += this_documents
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
    logging.info("Deleting outdated backups...")
    files = list_backups()
    outdated_backups = files[MAX_BACKUPS:]
    for file in outdated_backups:
        logging.info("Deleting outdated backup " + file)
        os.remove(file)

def get_last_backup_time():
    files = list_backups()
    if not files:
        return 0
    return os.path.getmtime(files[0])

def get_next_backup_time():
    last_time = get_last_backup_time()
    logging.info("Last time is {}".format(last_time))
    return (last_time // BACKUP_PERIOD_SEC + 1) * BACKUP_PERIOD_SEC + BACKUP_FRACTION_SEC

def maybe_run_backup():
    next_time = get_next_backup_time()
    logging.info("Next backup at {}, now it's {}".format(next_time, time.time()))
    if time.time() > next_time:
        fname = BACKUPS_DIR + "/backup_" + datetime.datetime.now().isoformat()
        run_backup(fname)
        restore_backup(fname)
        cleanup_backups()

def get_stats():
    stats = get_db_size()
    backups = list_backups()
    age = time.time() - get_last_backup_time()
    stats["size"] = os.path.getsize(backups[0])
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

