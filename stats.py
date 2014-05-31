import requests
import time
from prettytable import PrettyTable


data = requests.get("http://localhost:8888").json()
table = PrettyTable(["Total Success", "Total Failures", "Average Time"])
table.add_row([data["total_successful_queries"], data["total_failed_queries"], data["average_query_time"]])
print(table)
