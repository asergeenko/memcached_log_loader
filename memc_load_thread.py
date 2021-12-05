#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser

import functools
import time
import random
import datetime
from itertools import islice

from threading import Lock
from multiprocessing.dummy import Pool
from pymemcache.client import base
import queue


# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
#pip install pymemcache

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 400 # Lines

# Constances below are for exponential backoff algorithm used in 'retry' decorator

MIN_DELAY = 0.1
MAX_DELAY = 15 * 60
DELAY_FACTOR = 2
DELAY_JITTER = 0.1

AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

def retry(num_tries):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            delay = MIN_DELAY
            for _ in range(num_tries):
                try:
                    return f(*args, **kwargs)
                except (TimeoutError, ConnectionError) as e:
                    time.sleep(delay)
                    delay = min(delay*DELAY_FACTOR, MAX_DELAY)
                    delay = max(random.gauss(delay, DELAY_JITTER),MIN_DELAY)
            raise ConnectionError("Connection failed after %i tries"%(num_tries,))
        return wrapper
    return decorator

class MCRetryingClient:
    max_retries = 5

    def __init__(self,memc_addr):
        self.addr = memc_addr
        self.client = None

    def connect(self):
        self.client = base.Client(self.addr)

    @retry(max_retries)
    def set(self, key, value):
        self.client.set(key, value)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))

def insert_appsinstalled(memc_client, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_client.addr, key, str(ua).replace("\n", " ")))
        else:
            memc_client.set(key,packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_client.addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)

def read_batch_from_file(fp):
    return list(islice(fp,BATCH_SIZE))

class LogPipeline:
    def __init__(self, options,device_memc):
        self.errors = 0
        self.processed = 0
        self.queue = queue.Queue()
        self.lock = Lock()
        self.options = options
        self.mc_clients = {}
        self.device_memc = device_memc
        self.done = False

    def set_connection(self):
        for addr in self.device_memc.values():
            self.mc_clients[addr] = MCRetryingClient(addr)
            self.mc_clients[addr].connect()

    def consumer(self,idx):
        logging.info(f"Consumer {idx} started")

        while not self.done:
            batch = self.queue.get(timeout=0.05)
            if not batch:
                continue
            processed = errors = 0

            for line in batch:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_appsinstalled(line)
                if not appsinstalled:
                    errors += 1
                    continue
                memc_addr = self.device_memc.get(appsinstalled.dev_type)
                if not memc_addr:
                    errors += 1
                    logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                    continue

                ok = insert_appsinstalled(self.mc_clients[memc_addr], appsinstalled, self.options.dry)
                if ok:
                    processed += 1
                else:
                    errors += 1

            with self.lock:
                self.errors += errors
                self.processed += processed

            self.queue.task_done()


    def producer(self):
        logging.info("Producer started")

        for fn in glob.iglob(self.options.pattern):

            logging.info('Processing %s' % fn)

            fd = gzip.open(fn, 'rt',encoding='utf-8')
            batch = read_batch_from_file(fd)
            while batch:
                self.queue.put(batch)
                batch = read_batch_from_file(fd)
            self.queue.join()

            if not self.processed:
                self.processed = 0
                self.errors = 0
                fd.close()
                dot_rename(fn)
                continue

            err_rate = float(self.errors) / self.processed
            if err_rate < NORMAL_ERR_RATE:
                logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
            else:
                logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
            fd.close()
            dot_rename(fn)
        self.done = True

    def run(self):
        self.set_connection()
        pool = Pool(processes=self.options.workers + 1)
        pool.apply_async(self.producer)
        pool.map_async(self.consumer, range(self.options.workers))
        pool.close()
        pool.join()


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    pipeline = LogPipeline(options, device_memc)
    pipeline.run()

def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-w", "--workers", action="store", default=5)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        time_start = datetime.datetime.now()
        main(opts)
        print (f'Execution time: {datetime.datetime.now()-time_start}')
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
