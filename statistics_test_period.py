#!/usr/bin/env python3

import subprocess
import json
import psutil
from collections import defaultdict
import platform
import sys
from datetime import datetime

monitoring_period = sys.argv[1]
cpu_count = psutil.cpu_count()

# Creating a dictionary for collected statistics
statistics = {}

# Get the hostname and hwid to compose the path
hostname = platform.node()
hwid = subprocess.getoutput("/opt/rubackup/bin/rubackup_client hwid").split("\n")[2]
monitoring_files_path = "/opt/rubackup/monitoring/" + hostname + "_" + hwid + "/"

# Get pid of rubackup_client process and append it to the list of all pids(rubackup_client and childs)
pid_and_childs_pids = []

for proc in psutil.process_iter():
    if "rubackup_client" in proc.name():
        client_pid = proc.pid
        pid_and_childs_pids.append(client_pid)

# Get all pids of childs processes recursively
def get_all_child_process():
    for children in psutil.Process(client_pid).children(recursive=True):
        pid_and_childs_pids.append(children.pid)
    return pid_and_childs_pids


# Calculating total cpu times
def get_total_cpu_times():
    cpu_times = psutil.cpu_times()
    total_cpu_times = (
        cpu_times.user
        + cpu_times.nice
        + cpu_times.system
        + cpu_times.idle
        + cpu_times.iowait
        + cpu_times.irq
        + cpu_times.softirq
        + cpu_times.steal
    )
    return total_cpu_times


# Function, which get all clients stats
def get_client_stats_map(proc_pid):
    try:
        child_proc = psutil.Process(proc_pid)
        with child_proc.oneshot():
            client_times_user = child_proc.cpu_times().user
            client_times_system = child_proc.cpu_times().system
            child_io_read_KB = child_proc.io_counters().read_bytes / 1024
            child_io_write_KB = child_proc.io_counters().write_bytes / 1024
            child_memory_KB = child_proc.memory_info().rss

            return [client_times_user + client_times_system, child_io_read_KB, child_io_write_KB, child_memory_KB]
    except:
        print(f"File /proc/{proc_pid}/stat doesn't exist")
        return [0, 0, 0, 0]


# Get sum of all clients process statistics and all his child process, using list of all childs process statistics and theirs pids
def calculate_client_total_stat(list_of_child_pids):

    list_of_stat = map(get_client_stats_map, list_of_child_pids)
    list_of_stat_zipped = zip(*list_of_stat)
    # total_list = [sum(i) for i in list_of_stat_zipped]
    total_list = list(map(sum, list_of_stat_zipped))
    return {
        "total_client_cpu_times": total_list[0],
        "child_total_io_read_KB": total_list[1],
        "child_total_io_write_KB": total_list[2],
        "child_total_memory_KB": total_list[3],
    }


# Get total count of net usage for interface and rubackup_client process. For rubackup_client, we get statistics from a file /proc/<pid>/net/dev
def get_net_usage(inf="ens18"):  # change the inf variable according to the interface
    net_stat = psutil.net_io_counters(pernic=True, nowrap=True)[inf]
    net_in = net_stat.bytes_recv
    net_out = net_stat.bytes_sent

    return {
        "net_recieved": net_in,
        "net_sent": net_out,
    }


# Converting bytes to megabytes
def b_to_m(b):
    m = (b / 1024) / 1024
    return m


# This is a loop that repeats every monitoring_period to collect statistics
while True:
    # Call mpstat with one secong delay that output general_cpu_load
    mpstat_call = subprocess.Popen(["mpstat", "-o", "JSON", f"{monitoring_period}", "1"], stdout=subprocess.PIPE)

    # Read and decode mpstat output
    mpstat_call_output = mpstat_call.stdout.read().decode("utf8")

    # Get total count of cpu time
    total_cpu_times = get_total_cpu_times()

    # Get cpu load in percentage
    psutil_cpu_percent = psutil.cpu_percent()

    # Get a dic whith net_usage_metrics
    net_usage_output = get_net_usage()

    # Get total count of memory usage
    memory_call_output = psutil.virtual_memory()

    # Get total count of io read/write usage
    io_general = psutil.disk_io_counters(perdisk=True, nowrap=True)
    io_general_read_KB_total = io_general["sda"].read_bytes / 1024
    io_general_write_KB_total = io_general["sda"].write_bytes / 1024

    # Because mpstat outputs have JSON format there are use json.loads for read it
    mpstat_json_output = json.loads(mpstat_call_output)

    # Call the function for calculate client stats
    client_stats = calculate_client_total_stat(get_all_child_process())

    # Get general_cpu_load
    cpu_load_usr = 100 - (
        mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["cpu-load"][0]["idle"]
        + mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["cpu-load"][0]["iowait"]
    )
    mpstat_timestamp = mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["timestamp"]

    # Get memory usage metrics and convert to megabytes
    total_memory = memory_call_output.total
    available_memory = memory_call_output.available
    general_memory_usage_percent = memory_call_output.percent
    general_memory_usage = total_memory - available_memory
    general_memory_usage_m = b_to_m(general_memory_usage)

    client_memory_usage_m = b_to_m(client_stats["child_total_memory_KB"])
    client_memory_usage_percent = client_memory_usage_m / b_to_m(total_memory) * 100

    # Make monitoring filename
    # file_name = iostat_timestamp.replace(" ", "-").replace(":", "-")
    file_name = str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))

    # The collected metrics are added to the dictionary where first level keys it is a formatted outup of "date" utility, which also is a name of monitoring file.
    statistics[file_name] = {
        "disk_io_usage": {
            "io_general_write_KB_total": io_general_write_KB_total,
            "io_general_read_KB_total": io_general_read_KB_total,
            "io_general_write_KB_period": 0,
            "io_general_read_KB_period": 0,
            "client_io_usage_read_KB": 0,
            "client_io_usage_write_KB": 0,
        },
        "net_usage_total": net_usage_output,
        "net_usage_rates": defaultdict(int),
        "cpu_usage": {
            "psutil_cpu_percent": psutil_cpu_percent,
            "mpstat_timestamp": mpstat_timestamp,
            "cpu_load_usr": cpu_load_usr,
            "total_cpu_times": total_cpu_times,
            "client_cpu_load_percent": 0,
        },
        "memory_usage": {
            "general_memory_usage_m": general_memory_usage_m,
            "general_memory_usage_percent": general_memory_usage_percent,
            "client_memory_usage_percent": client_memory_usage_percent,
            "client_memory_usage_m": client_memory_usage_m,
        },
        "client_stats": client_stats,
    }

    # If lenght of statistics dictionary < 2, then metrics can't be calculated.
    # If lenght less than 2, then cycle is started again and childs pids deleting from list
    if len(statistics) < 2:
        del pid_and_childs_pids[1:]
        continue
    elif len(statistics) >= 2:
        del pid_and_childs_pids[1:]

    # Get a first, second key name in dictionary and path to RuBackup monitoring files
    key_name = list(statistics.keys())[0]
    key_name_next = list(statistics.keys())[1]
    file_path = monitoring_files_path + key_name

    # Calculating disk_io_usage of rubackup_client and childs processes
    io_general_read_KB_period = (
        statistics[key_name_next]["disk_io_usage"]["io_general_read_KB_total"]
        - statistics[key_name]["disk_io_usage"]["io_general_read_KB_total"]
    )
    io_general_write_KB_period = (
        statistics[key_name_next]["disk_io_usage"]["io_general_write_KB_total"]
        - statistics[key_name]["disk_io_usage"]["io_general_write_KB_total"]
    )

    # Calculating disk_io_usage of rubackup_client and childs processes
    io_client_read = (
        statistics[key_name_next]["client_stats"]["child_total_io_read_KB"]
        - statistics[key_name]["client_stats"]["child_total_io_read_KB"]
    )
    io_client_write = (
        statistics[key_name_next]["client_stats"]["child_total_io_write_KB"]
        - statistics[key_name]["client_stats"]["child_total_io_write_KB"]
    )

    # Calculating general_net_usage and client_net_usage
    next_net_rates_in = (
        statistics[key_name_next]["net_usage_total"]["net_recieved"]
        - statistics[key_name]["net_usage_total"]["net_recieved"]
    ) / 1024
    next_net_rates_out = (
        statistics[key_name_next]["net_usage_total"]["net_sent"] - statistics[key_name]["net_usage_total"]["net_sent"]
    ) / 1024

    # Calculating total_cpu_times, client_cpu_times and client_cpu_usage_percent for monitoring period
    cpu_times_period = (
        statistics[key_name_next]["cpu_usage"]["total_cpu_times"]
        - statistics[key_name]["cpu_usage"]["total_cpu_times"]
    )
    client_cpu_times_period = (
        statistics[key_name_next]["client_stats"]["total_client_cpu_times"]
        - statistics[key_name]["client_stats"]["total_client_cpu_times"]
    )
    client_cpu_load_percent = client_cpu_times_period / (cpu_times_period / 100)

    # Appendind calculated disk_io_usage, cpu_usage and net_usage to dictionary

    statistics[key_name_next]["disk_io_usage"]["io_general_write_KB_period"] = io_general_write_KB_period
    statistics[key_name_next]["disk_io_usage"]["io_general_read_KB_period"] = io_general_read_KB_period

    statistics[key_name_next]["disk_io_usage"]["client_io_usage_read_KB"] = io_client_read
    statistics[key_name_next]["disk_io_usage"]["client_io_usage_write_KB"] = io_client_write

    statistics[key_name_next]["cpu_usage"]["client_cpu_load_percent"] = client_cpu_load_percent

    statistics[key_name_next]["net_usage_rates"] = {
        "net_recieved_KB": next_net_rates_in,
        "net_sent_KB": next_net_rates_out,
    }

    # Try opening RuBackup monitoring file and if file is exist, then print all metrics.
    # If file not exist, then cycle started again
    try:
        with open(file_path, "r") as j:
            monitoring_file_data = json.load(j)
            print(file_path)
            print("timestamp_before" + " " + monitoring_file_data["timestamp_before"])
            print("timestamp_after" + " " + monitoring_file_data["timestamp_after"])
            print("mpstat_timestamp" + " " + statistics[key_name]["cpu_usage"]["mpstat_timestamp"])
            print("\n")
            print("psutil_general_cpu_percent" + " " + str(statistics[key_name]["cpu_usage"]["psutil_cpu_percent"]))
            print("general_cpu_usage" + " " + monitoring_file_data["general_cpu_usage"])
            print("client_cpu_usage" + " " + monitoring_file_data["client_cpu_usage"])
            print("mpstat_general_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["cpu_load_usr"]))
            print("test_client_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["client_cpu_load_percent"]))
            print("\n")
            print("general_io_r" + " " + monitoring_file_data["general_io_usage_r"])
            print(
                "psutil_general_io_r" + " " + str(statistics[key_name]["disk_io_usage"]["io_general_read_KB_period"])
            )
            print("client_io_r" + " " + monitoring_file_data["client_io_usage_r"])
            print("psutil_client_io_r" + " " + str(statistics[key_name]["disk_io_usage"]["client_io_usage_read_KB"]))
            print("client_io_w" + " " + monitoring_file_data["client_io_usage_w"])
            print("psutil_client_io_w" + " " + str(statistics[key_name]["disk_io_usage"]["client_io_usage_write_KB"]))
            print("general_io_w" + " " + monitoring_file_data["general_io_usage_w"])
            print(
                "psutil_general_io_w" + " " + str(statistics[key_name]["disk_io_usage"]["io_general_write_KB_period"])
            )
            print("\n")
            print("general_net_w" + " " + monitoring_file_data["general_net_usage_w"])
            print("psutil_net_w" + " " + str(statistics[key_name]["net_usage_rates"]["net_sent_KB"]))
            print("general_net_r" + " " + monitoring_file_data["general_net_usage_r"])
            print("psutil_net_r" + " " + str(statistics[key_name]["net_usage_rates"]["net_recieved_KB"]))
            print("\n")
            print("general_ram_%" + " " + monitoring_file_data["general_ram_usage"])
            print(
                "psutil_general_ram_%"
                + " "
                + str(statistics[key_name]["memory_usage"]["general_memory_usage_percent"])
            )
            print("client_ram_%" + " " + monitoring_file_data["client_ram_usage"])
            print(
                "psutil_client_ram_%" + " " + str(statistics[key_name]["memory_usage"]["client_memory_usage_percent"])
            )
            print("general_ram_m" + " " + monitoring_file_data["general_ram_usage_m"])
            print("psutil_general_ram_m" + " " + str(statistics[key_name]["memory_usage"]["general_memory_usage_m"]))
            print("client_ram_m" + " " + monitoring_file_data["client_ram_usage_m"])
            print("psutil_client_ram_m" + " " + str(statistics[key_name]["memory_usage"]["client_memory_usage_m"]))
            print("\n")

            # Deleting first record in dictionary
            statistics.pop(list(statistics.keys())[0])
            print("------------------")
    except IOError as e:
        statistics.pop(list(statistics.keys())[0])
        print(e)
        print("------------------")
        continue
