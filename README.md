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
    usage: impala_load_test.py [-h] --query_file f --threads t --impala_hosts h
                                [--stats_port p]


## Example

    $ cat queries.txt
    SELECT 1 + 1;
    SELECT 2 + 2;
    SELECT 3 + 3;

    $ python impala_load_test.py --query_file=queries.txt --threads=5 --impala_hosts=10.16.181.113,10.16.181.114,10.16.181.115
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

You can also run the stats.py script which prints the current stats from HTTP to the terminal in a nice pretty table.


    $ python stats.py
    ========== Global Stats ==========
    +---------------+----------------+--------------------+----------------+----------------------------+
    | Total Success | Total Failures | Average Query Time | Total Runtime  | Estimated Queries Per Hour |
    +---------------+----------------+--------------------+----------------+----------------------------+
    |      1743     |       0        |   1.02163942459    | 0:02:21.109358 |           44502            |
    +---------------+----------------+--------------------+----------------+----------------------------+



    ========== Individual Stats==========
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
    |   Thread  |        Impala Host         | Total Success | Total Failures |  Average Time  | Running Query Time |
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
    | Thread-19 | cottoop08.mtv.cloudera.com |       71      |       0        | 0.97440288772  |   0.726090908051   |
    | Thread-18 | cottoop07.mtv.cloudera.com |       72      |       0        | 0.95951459474  |                    |
    | Thread-13 | cottoop02.mtv.cloudera.com |       66      |       0        | 1.12178278692  |   0.893409013748   |
    | Thread-12 | cottoop01.mtv.cloudera.com |       69      |       0        | 1.05488447867  |                    |
    | Thread-11 | cottoop10.mtv.cloudera.com |       72      |       0        | 0.957974814706 |                    |
    | Thread-10 | cottoop09.mtv.cloudera.com |       72      |       0        | 0.957079198625 |                    |
    | Thread-17 | cottoop06.mtv.cloudera.com |       71      |       0        | 0.984555711209 |  0.00124502182007  |
    | Thread-16 | cottoop05.mtv.cloudera.com |       68      |       0        | 1.07797095004  |                    |
    | Thread-15 | cottoop04.mtv.cloudera.com |       68      |       0        | 1.05532084844  |   1.15838599205    |
    | Thread-14 | cottoop03.mtv.cloudera.com |       70      |       0        | 1.02727906023  |                    |
    |  Thread-9 | cottoop08.mtv.cloudera.com |       72      |       0        | 0.958285106553 |                    |
    |  Thread-8 | cottoop07.mtv.cloudera.com |       72      |       0        | 0.948704643382 |   0.62193608284    |
    |  Thread-7 | cottoop06.mtv.cloudera.com |       72      |       0        | 0.96268357171  |                    |
    |  Thread-6 | cottoop05.mtv.cloudera.com |       69      |       0        | 1.05619531438  |                    |
    |  Thread-5 | cottoop04.mtv.cloudera.com |       68      |       0        | 1.07776907612  |                    |
    |  Thread-4 | cottoop03.mtv.cloudera.com |       68      |       0        |  1.0786908304  |                    |
    |  Thread-3 | cottoop02.mtv.cloudera.com |       69      |       0        | 1.04992320572  |                    |
    |  Thread-2 | cottoop01.mtv.cloudera.com |       68      |       0        | 1.06266536432  |   0.683424949646   |
    | Thread-26 | cottoop05.mtv.cloudera.com |       70      |       0        | 1.00484127998  |   0.559206962585   |
    | Thread-24 | cottoop03.mtv.cloudera.com |       70      |       0        | 1.00543683938  |   0.520417928696   |
    | Thread-25 | cottoop04.mtv.cloudera.com |       68      |       0        | 1.06849292797  |  0.0797538757324   |
    | Thread-22 | cottoop01.mtv.cloudera.com |       68      |       0        | 1.07748266879  |                    |
    | Thread-23 | cottoop02.mtv.cloudera.com |       68      |       0        | 1.07346872722  |                    |
    | Thread-20 | cottoop09.mtv.cloudera.com |       71      |       0        | 0.97144339454  |   0.930362939835   |
    | Thread-21 | cottoop10.mtv.cloudera.com |       71      |       0        | 0.974137333077 |   0.734420061111   |
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
