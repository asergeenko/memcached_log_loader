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
from queue import Queue

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2

# pip install pymemcache

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 400  # Lines

# Constants below are for exponential backoff algorithm used in 'retry' decorator

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
                    delay = min(delay * DELAY_FACTOR, MAX_DELAY)
                    delay = max(random.gauss(delay, DELAY_JITTER), MIN_DELAY)
            raise ConnectionError("Connection failed after %i tries" % (num_tries,))

        return wrapper

    return decorator


class MCRetryingClient:
    max_retries = 5

    def __init__(self, memc_addr):
        self.addr = memc_addr
        self.client = None

    def connect(self):
        self.client = base.Client(self.addr)

    @retry(max_retries)
    def set_many(self, values):
        return self.client.set_many(values)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_client: base.Client, appsinstalled_list, dry_run=False):
    packed_dict = {}
    for appsinstalled in appsinstalled_list:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()

        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_client.addr, key, str(ua).replace("\n", " ")))
        else:
            packed_dict[key] = packed

    if not dry_run:
        try:
            not_ok = len(memc_client.set_many(packed_dict))
            return len(packed_dict) - not_ok, not_ok
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (memc_client.addr, e))
            return 0, len(packed_dict)


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
    return list(islice(fp, BATCH_SIZE))


def consumer(idx, queue, lock, stats, device_memc, mc_clients, dry):
    logging.info(f"Consumer {idx} started")

    while not stats['done']:
        batch = queue.get(timeout=0.05)
        if not batch:
            continue
        processed = errors = 0

        apps = collections.defaultdict(list)

        for line in batch:
            line = line.strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error("Unknown device type: %s" % appsinstalled.dev_type)
                continue

            apps[memc_addr].append(appsinstalled)

        for memc_addr, appsinstalled_list in apps.items():
            ok, not_ok = insert_appsinstalled(mc_clients[memc_addr], appsinstalled_list, dry)
            processed += ok
            errors += not_ok

        with lock:
            stats['errors'] += errors
            stats['processed'] += processed

        queue.task_done()


def producer(queue, stats, pattern):
    logging.info("Producer started")

    for fn in glob.iglob(pattern):
        stats['processed'] = 0
        stats['errors'] = 0
        logging.info('Processing %s' % fn)

        fd = gzip.open(fn, 'rt', encoding='utf-8')
        batch = read_batch_from_file(fd)
        while batch:
            queue.put(batch)
            batch = read_batch_from_file(fd)
        queue.join()

        if not stats['processed']:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(stats['errors']) / stats['processed']
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)
    stats['done'] = True


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    stats = {'errors': 0, 'processed': 0, 'done': False}
    queue = Queue()
    lock = Lock()
    mc_clients = {}

    for addr in device_memc.values():
        mc_clients[addr] = MCRetryingClient(addr)
        mc_clients[addr].connect()

    pool = Pool(processes=options.workers + 1)
    pool.apply_async(producer, (queue, stats, options.pattern))
    pool.map_async(lambda idx: consumer(idx, queue, lock, stats, device_memc, mc_clients, options.dry),
                   range(options.workers))
    pool.close()
    pool.join()


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
        print(f'Execution time: {datetime.datetime.now() - time_start}')
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
