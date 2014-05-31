# Simple Impala Workload Simulator

## Requirements

1. Impyla (http://github.com/cloudera/impyla <-- install manually)
2. Tornado
3. Requests
4. Prettytable

## Instructions
1. Create a file containing queries that you want executed at random
2. Execute the impala_load_test.py against your Impala cluster, specifying the
number of threads/concurrent queries.


    $ python impala_load_test.py
    usage: impala_load_test.py [-h] --query_file f --threads t --impala_host h
                                [--stats_port p]


## Example

    $ cat queries.txt
    SELECT 1 + 1;
    SELECT 2 + 2;
    SELECT 3 + 3;

    $ python impala_load_test.py --query_file=queries.txt --threads=5 --impala_host=10.16.181.113
    $ python impala_load_test.py --query_file=queries.txt --threads=5 --impala_host=10.16.181.113
    Stats HTTP handler listening => http://localhost:8888
    Starting Thread-3
    Starting Thread-4
    Starting Thread-5
    Starting Thread-6
    Starting Thread-7
    ...
    Thread-7: query #1 running
    Thread-6: query #1 running
    Thread-5: query #1 running
    Thread-5: query #1 finished
    Thread-6: query #1 finished
    Thread-3: query #1 finished
    Thread-4: query #1 finished
    ...


### Stats
You can view JSON stats about query failures, times, etc from the HTTP thread
( http://localhost:8888 <-- the machine running the simulator).
