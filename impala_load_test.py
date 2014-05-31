#!/usr/bin/env python
# Copyright 2014 (c) Cloudera
import argparse
import datetime
from threading import Thread
import time
import random
import signal

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

    def __init__(self, queries, num_threads, impala_host, stats_port):
        # Define the number of threads to launch
        Thread.__init__(self)
        self.__num_threads = num_threads
        self.__queries = queries
        self.__finished = False
        self.__impala_threads = []
        self.__impala_host = impala_host
        self.__stats_port = stats_port

    def get_new_connection(self):
        return connect(host=self.__impala_host, port=21050)

    def run(self):
        """
        Execute Impala query threads which run randomized queries
        """
        # Start HTTP Stats Thread
        self.__http_thread = HttpThread(self.__stats_port, stats_method=self.stats)
        self.__http_thread.start()

        for _ in range(self.__num_threads):
            query_thread = ImpalaQuery(self.get_new_connection(), self.__queries)
            self.__impala_threads.append(query_thread)
            print("Starting " + query_thread.name)

        for query_thread in self.__impala_threads:
            query_thread.start()

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
        average_query_time = sum(i["average_query_time"] for i in stats.values()) / len(self.__impala_threads)
        stats["total_successful_queries"] = total_successful_queries
        stats["total_failed_queries"] = total_failed_queries
        stats["average_query_time"] = average_query_time
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
                self.say("query #%d running" % num_queries)
                self.__active_cursor.execute(query)
                _ = self.__active_cursor.fetchall()
                self.__active_cursor.close()
                self.say("query #%d finished" % num_queries)
                self.__running_query = False
                self.__running_end_time = time.time()
                self.__successful += 1
                self.__query_times.append((self.__running_end_time - self.__running_start_time))
            except Exception as e:
                self.say("query #%d failed" % num_queries)
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
                    average_query_time=self.average_run_time)
        # See if we have an active query, if so, find out how long it's been running
        if self.__running_query:
            stats["currently_running_query_time"] = time.time() - self.__running_start_time
            stats["currently_running_query"] = True
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
    argparser.add_argument("--impala_host", metavar="h", type=str, required=True)
    argparser.add_argument("--stats_port", metavar="p", type=str, default=8888)
    args = argparser.parse_args()
    queries_file = open(args.query_file, "r")
    queries = queries_file.read().replace("\n", "").split(";")
    queries.pop(-1)  # remove trailing empty element
    scheduler = ImpalaQueryScheduler(queries, args.threads, args.impala_host, args.stats_port)

    # Keep an empty place holder for the final stats
    stats = None

    def signal_handler(signal, frame):
        global stats
        stats = scheduler.stats()
        scheduler.shutdown()
        while not scheduler.finished:
            time.sleep(.10)

    scheduler.start()
    signal.signal(signal.SIGINT, signal_handler)
    signal.pause()

    # Print the overall stats
    total_successful_queries = sum(i["successful"] for i in stats.values())
    total_failed_queries = sum(i["failures"] for i in stats.values())
    total_threads = len(stats.values())
    print(stats.values()[0]["average_query_time"])
    average_query_time = sum(i["average_query_time"] for i in stats.values()) / total_threads
    failed_queries = []
    for i in stats.values():
        for f in i["failed_queries"]:
            failed_queries.append(f)

    print("\n\n")
    print("=== Ending Stats ====")
    print("Total Threads: %d" % total_threads)
    print("Total Successful Queries: %d" % total_successful_queries)
    print("Total Failed Queries: %d" % total_failed_queries)
    print("Average Query Time: %f seconds" % average_query_time)
    if failed_queries:
        print("\n\nFailed Queries:")
        for f in failed_queries:
            print("== Time==\n%s" % f["failed_time"])
            print("== Query ==\n%s" % f["query"])
            print("== Exception ==\n%s" % f["exception"])
