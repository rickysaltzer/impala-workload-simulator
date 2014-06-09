import requests
from prettytable import PrettyTable
import sys


def get_stats_tables(url=None):
    data = requests.get(url or "http://localhost:8888").json()
    global_table = PrettyTable(
        ["Total Success", "Total Failures", "Average Query Time", "Total Runtime", "Estimated Queries Per Hour"])
    global_table.add_row(
        [data["total_successful_queries"], data["total_failed_queries"], data["average_query_time"],
         data["total_runtime"],
         data["queries_per_hour"]])

    individual_table = PrettyTable(
        ["Thread", "Impala Host", "Total Success", "Total Failures", "Average Time", "Running Query Time",
         "Estimated Queries Per Hour"])
    for i in data.keys():
        if i.startswith("Thread"):
            running_query_time = ""
            if data[i]["currently_running_query"]:
                running_query_time = data[i]["currently_running_query_time"]
            individual_table.add_row(
                [i, data[i]["impala_host"], data[i]["successful"], data[i]["failures"], data[i]["average_query_time"],
                 running_query_time, data[i]["queries_per_hour"]])

    return {"global": global_table, "individual": individual_table}


def print_stats(stats):
    print("=" * 10 + " Global Stats " + "=" * 10)
    print(stats["global"])
    print("\n\n")
    print("=" * 10 + " Individual Stats" + "=" * 10)
    print(stats["individual"])


if __name__ == "__main__":
    url = None
    if len(sys.argv) > 1:
        if sys.argv[1].startswith("http"):
            url = sys.argv[1]
        else:
            url = "http://" + sys.argv[1]
    print_stats(get_stats_tables(url=url))
