import requests
import time
from prettytable import PrettyTable


print("=" * 10 + "Overall Stats" + "=" * 10)
data = requests.get("http://localhost:8888").json()
table = PrettyTable(["Total Success", "Total Failures", "Average Time"])
table.add_row([data["total_successful_queries"], data["total_failed_queries"], data["average_query_time"]])
print(table)

print("\n\n")
print("=" * 10 + "Individual Stats" + "=" * 10)
table = PrettyTable(["Thread", "Impala Host", "Total Success", "Total Failures", "Average Time", "Running Query Time"])
for i in data.keys():
    if i.startswith("Thread"):
        running_query_time = ""
        if data[i]["currently_running_query"]:
            running_query_time = data[i]["currently_running_query_time"]
        table.add_row([i, data[i]["impala_host"], data[i]["successful"], data[i]["failures"], data[i]["average_query_time"], running_query_time])
print(table)
