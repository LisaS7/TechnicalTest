from collections import defaultdict
from datetime import datetime, timedelta

INPUT_FILE = "./sample.log"
OUTPUT_FILE = "./output.txt"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

with open(INPUT_FILE) as f:
    lines = f.readlines()

groups = defaultdict(list)
for line in lines:
    split_line = line.split()

    parsed_datetime = datetime.strptime(split_line[0], DATETIME_FORMAT)
    split_line[0] = parsed_datetime
    guid = split_line[1]

    groups[guid].append(split_line)


response_times = []
for guid in groups:
    for request_time, guid, action, _, _, status, _, server_id in groups[guid]:
        match action.upper():
            case "GET" | "POST":
                start_time = request_time
            case "HANDLE":
                handle_time = request_time
                time_difference = request_time - start_time
            case "RESPOND":
                time_difference = request_time - handle_time

        if action.upper() in ["HANDLE", "RESPOND"]:
            server_record = next(
                (
                    record
                    for record in response_times
                    if record["server_id"] == server_id
                ),
                None,
            )

            if server_record:
                server_record["total_time"] += time_difference
                server_record["count"] += 1
            else:
                new_record = {
                    "server_id": server_id,
                    "total_time": time_difference,
                    "count": 1,
                }
                response_times.append(new_record)


for record in response_times:
    record["average_time"] = record["total_time"] / record["count"]

overall_average = sum(
    (record["average_time"] for record in response_times), timedelta(0)
) / len(response_times)

slowest_servers = [
    record for record in response_times if record["average_time"] >= overall_average
]

sorted_records = sorted(slowest_servers, key=lambda x: x["average_time"], reverse=True)

with open(OUTPUT_FILE, "w+") as out:
    out.write(
        f"OVERALL AVERAGE RESPONSE TIME: {overall_average.total_seconds() * 1e6} μs"
        + "\n"
    )
    for record in sorted_records:
        output = f"Server Id: {record['server_id']}, Average response time: {record['average_time'].total_seconds() * 1e6} μs"
        out.write(output + "\n")
