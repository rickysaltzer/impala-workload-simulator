#!/usr/bin/env python
# Copyright 2014 (c) Cloudera
import argparse
import datetime
from threading import Thread
import time
import random
import signal
from stats import get_stats_tables, print_stats
from impala.dbapi import connect
import tornado.ioloop
import simplejson as json
import tornado.web

# Date Format
DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

# Time in-between connections
TIME_BETWEEN_QUERIES = 1  # Seconds


class ImpalaQueryScheduler(Thread):
    """
    Thread responsible for launching and monitoring ImpalaQuery threads
    """

    def __init__(self, queries, num_threads, impala_hosts, stats_port):
        # Define the number of threads to launch
        Thread.__init__(self)
        self.__num_threads = num_threads
        self.__queries = queries
        self.__finished = False
        self.__impala_threads = []
        self.__impala_hosts = impala_hosts
        self.__connection_pool = (host for host in self.__impala_hosts)
        self.__stats_port = stats_port
        self.__start_time = datetime.datetime.now()

    def get_new_connection(self):
        # Use a generator to keep a revolving iteration of the impala hosts, guaranteeing even connection distribution
        try:
            impala_host = self.__connection_pool.next()
        except StopIteration:
            # This means we've reached the end of the Python generator, let's reset it
            self.__connection_pool = (host for host in self.__impala_hosts)
            impala_host = self.__connection_pool.next()
        connection = connect(host=impala_host, port=21050)
        connection.host = impala_host  # Slap the hostname inside the connection object
        return connection

    def run(self):
        """
        Execute Impala query threads which run randomized queries
        """
        for _ in range(self.__num_threads):
            query_thread = ImpalaQuery(self.get_new_connection(), self.__queries)
            self.__impala_threads.append(query_thread)
            print("Starting " + query_thread.name)

        for query_thread in self.__impala_threads:
            query_thread.start()

        # Start HTTP Stats Thread
        self.__http_thread = HttpThread(self.__stats_port, stats_method=self.stats)
        self.__http_thread.start()

    def shutdown(self):
        """
        Shut down all the threads
        """
        print("Shutting down...")
        self.__http_thread.shutdown()
        # Shutdown the threads
        for thread in self.__impala_threads:
            thread.shutdown()

        # Wait for the threads to shutdown
        print("Waiting for %d threads to to shutdown..." % len(self.__impala_threads))
        for thread in self.__impala_threads:
            while thread.stats()["running"]:
                time.sleep(.10)
            print(thread.name + " shutdown")

        self.__finished = True
        print("Scheduler shutdown")

    @property
    def finished(self):
        return self.__finished

    def stats(self):
        stats = {}
        for query_thread in self.__impala_threads:
            stats[query_thread.name] = query_thread.stats()
        total_successful_queries = sum([i["successful"] for i in stats.values()])
        total_failed_queries = sum([i["failures"] for i in stats.values()])
        average_query_time = sum([i["average_query_time"] for i in stats.values() if i["average_query_time"]]) / len(
            self.__impala_threads)
        stats["total_successful_queries"] = total_successful_queries
        stats["total_failed_queries"] = total_failed_queries
        stats["average_query_time"] = average_query_time

        # basic estiamtion of the "queries per hour" metric
        total_runtime = (datetime.datetime.now() - self.__start_time)
        stats["total_runtime"] = str(total_runtime)
        if total_successful_queries > 0:
            stats["queries_per_hour"] = int(
                (float(total_successful_queries) / float(total_runtime.seconds)) * (60.0 * 60.0))
        else:
            stats["queries_per_hour"] = 0
        return stats


class ImpalaQuery(Thread):
    def __init__(self, connection, queries):
        """
        Given an Impala connection and an array of queries, continously
        execute a random query against Impala.
        """
        Thread.__init__(self)
        self.__connection = connection
        self.__queries = queries
        self.__shutdown = False
        self.__failures = 0
        self.__successful = 0
        self.__start_time = datetime.datetime.now()
        self.__running = False
        self.__failed_queries = []
        self.__query_times = []
        self.__running_query = False

    def random_query(self):
        return random.choice(self.__queries)

    def say(self, message):
        print(self.name + ": " + message)

    def run(self):
        self.__running = True
        num_queries = 0
        while not self.__shutdown:
            # Try to run a query
            query = self.random_query()
            num_queries += 1
            try:
                self.__active_cursor = self.__connection.cursor()
                self.__running_start_time = time.time()
                self.__running_query = True
                self.__active_cursor.execute(query)
                _ = self.__active_cursor.fetchall()
                self.__active_cursor.close()
                self.__running_query = False
                self.__running_end_time = time.time()
                self.__successful += 1
                self.__query_times.append((self.__running_end_time - self.__running_start_time))
            except Exception as e:
                self.__failures += 1
                self.__failed_queries.append(
                    dict(query=query, exception=e.message, failed_time=datetime.datetime.now().strftime(DATE_FORMAT)))
            time.sleep(TIME_BETWEEN_QUERIES)
        print(self.name + " shutdown")
        self.__running = False

    def shutdown(self):
        self.say("closing impala connection")
        self.__connection.close()
        self.__shutdown = True
        self.say("closed impala connection")

    @property
    def average_run_time(self):
        if self.__query_times:
            return sum(self.__query_times) / len(self.__query_times)

    def stats(self):
        stats = dict(failures=self.__failures, successful=self.__successful,
                     start_time=self.__start_time.strftime(DATE_FORMAT), running=self.__running,
                     failed_queries=self.__failed_queries,
                     average_query_time=self.average_run_time, impala_host=self.__connection.host)
        # See if we have an active query, if so, find out how long it's been running
        if self.__running_query:
            stats["currently_running_query_time"] = time.time() - self.__running_start_time
            stats["currently_running_query"] = True
        else:
            stats["currently_running_query"] = False
        return stats


class HttpThread(Thread):
    def __init__(self, port, stats_method):
        Thread.__init__(self)
        self.__stats_method = stats_method
        self.__port = port

    def run(self):
        stats_http = tornado.web.Application([(r"/", self.StatsHttpHandler, dict(stats_method=self.__stats_method))])
        stats_http.listen(self.__port)
        print("Stats HTTP handler listening => http://localhost:%d" % self.__port)
        tornado.ioloop.IOLoop.instance().start()

    def shutdown(self):
        tornado.ioloop.IOLoop.instance().stop()
        print("http thread stopped")

    class StatsHttpHandler(tornado.web.RequestHandler):
        def initialize(self, stats_method):
            self.__stats_method = stats_method

        def get(self):
            self.write(json.dumps(self.__stats_method()))


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Simple Impala workload simulator.")
    argparser.add_argument("--query_file", metavar="f", type=str, required=True)
    argparser.add_argument("--threads", metavar="t", type=int, required=True)
    argparser.add_argument("--impala_hosts", metavar="h", type=str, required=True)
    argparser.add_argument("--stats_port", metavar="p", type=str, default=8888)
    args = argparser.parse_args()
    queries_file = open(args.query_file, "r")
    queries = queries_file.read().replace("\n", "").split(";")
    queries.pop(-1)  # remove trailing empty element
    scheduler = ImpalaQueryScheduler(queries, args.threads, args.impala_hosts.split(","), args.stats_port)

    # Keep an empty place holder for the final stats
    stats = None

    def signal_handler(signal, frame):
        global stats
        stats = get_stats_tables()
        scheduler.shutdown()
        while not scheduler.finished:
            time.sleep(.10)

    scheduler.start()
    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()

    print("\n\n")
    print_stats(stats)
