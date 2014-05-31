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
    ==========Overall Stats==========
    +---------------+----------------+----------------+
    | Total Success | Total Failures |  Average Time  |
    +---------------+----------------+----------------+
    |      560      |       0        | 0.858408534574 |
    +---------------+----------------+----------------+


    ==========Individual Stats==========
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
    |   Thread  |        Impala Host         | Total Success | Total Failures |  Average Time  | Running Query Time |
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
    | Thread-19 | cottoop07.mtv.cloudera.com |       23      |       0        | 0.851490445759 |                    |
    | Thread-18 | cottoop06.mtv.cloudera.com |       22      |       0        | 0.866624507037 |   0.545561075211   |
    | Thread-13 | cottoop01.mtv.cloudera.com |       23      |       0        | 0.844035656556 |                    |
    | Thread-12 | cottoop10.mtv.cloudera.com |       22      |       0        | 0.856634811922 |   0.842869997025   |
    | Thread-11 | cottoop09.mtv.cloudera.com |       22      |       0        | 0.871873714707 |   0.418411016464   |
    | Thread-10 | cottoop08.mtv.cloudera.com |       22      |       0        | 0.864315520633 |   0.579195022583   |
    | Thread-17 | cottoop05.mtv.cloudera.com |       23      |       0        | 0.854346130205 |                    |
    | Thread-16 | cottoop04.mtv.cloudera.com |       22      |       0        | 0.869141762907 |   0.53549695015    |
    | Thread-15 | cottoop03.mtv.cloudera.com |       23      |       0        | 0.853340097096 |                    |
    | Thread-14 | cottoop02.mtv.cloudera.com |       22      |       0        | 0.869811794975 |   0.471101999283   |
    |  Thread-9 | cottoop07.mtv.cloudera.com |       22      |       0        | 0.857216444882 |   0.729597091675   |
    |  Thread-8 | cottoop06.mtv.cloudera.com |       22      |       0        | 0.867344639518 |   0.541918039322   |
    |  Thread-7 | cottoop05.mtv.cloudera.com |       23      |       0        | 0.853373413501 |                    |
    |  Thread-6 | cottoop04.mtv.cloudera.com |       23      |       0        | 0.851857931718 |                    |
    |  Thread-5 | cottoop03.mtv.cloudera.com |       22      |       0        | 0.86438723044  |   0.582756996155   |
    |  Thread-4 | cottoop02.mtv.cloudera.com |       22      |       0        | 0.873402758078 |   0.257457017899   |
    |  Thread-3 | cottoop01.mtv.cloudera.com |       23      |       0        | 0.844295304755 |                    |
    | Thread-26 | cottoop04.mtv.cloudera.com |       22      |       0        | 0.85479426384  |   0.729365825653   |
    | Thread-27 | cottoop05.mtv.cloudera.com |       23      |       0        | 0.840302270392 |                    |
    | Thread-24 | cottoop02.mtv.cloudera.com |       22      |       0        | 0.873652143912 |   0.230617046356   |
    | Thread-25 | cottoop03.mtv.cloudera.com |       23      |       0        | 0.852507166241 |                    |
    | Thread-22 | cottoop10.mtv.cloudera.com |       22      |       0        | 0.856391516599 |   0.777354001999   |
    | Thread-23 | cottoop01.mtv.cloudera.com |       23      |       0        | 0.845959922542 |                    |
    | Thread-20 | cottoop08.mtv.cloudera.com |       22      |       0        | 0.863940336488 |   0.581480979919   |
    | Thread-21 | cottoop09.mtv.cloudera.com |       22      |       0        | 0.859173579649 |   0.645567178726   |
    +-----------+----------------------------+---------------+----------------+----------------+--------------------+
