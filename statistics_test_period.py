#!/usr/bin/env python3

import subprocess
import json
import psutil
from collections import defaultdict
import platform
import sys
from datetime import datetime
from functools import reduce

# from multiprocessing import Pool

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


def get_client_stats_map(proc_pid):

    child_proc = psutil.Process(proc_pid)
    with child_proc.oneshot():
        try:
            client_times_user = child_proc.cpu_times().user
            client_times_system = child_proc.cpu_times().system
            child_io_read_KB = child_proc.io_counters().read_bytes / 1024
            child_io_write_KB = child_proc.io_counters().write_bytes / 1024

            return [client_times_user + client_times_system, child_io_read_KB, child_io_write_KB]
        except:
            print(f"File /proc/{proc_pid}/stat doesn't exist")
            return [0, 0, 0]


def calculate_client_total_stat(list_of_child_pids):

    list_of_stat = map(get_client_stats_map, list_of_child_pids)
    list_of_stat_zipped = zip(*list_of_stat)
    # total_list = [sum(i) for i in list_of_stat_zipped]
    total_list = list(map(sum, list_of_stat_zipped))
    return {
        "total_client_cpu_times": total_list[0],
        "child_total_io_read_KB": total_list[1],
        "child_total_io_write_KB": total_list[2],
    }


# Get total count of net usage for interface and rubackup_client process. For rubackup_client, we get statistics from a file /proc/<pid>/net/dev
def get_net_usage(inf="ens18"):  # change the inf variable according to the interface
    net_stat = psutil.net_io_counters(pernic=True, nowrap=True)[inf]
    net_in = net_stat.bytes_recv
    net_out = net_stat.bytes_sent

    net_usage_stats = {
        "net_recieved": net_in,
        "net_sent": net_out,
    }

    return net_usage_stats


# Converting bytes to megabytes
def b_to_m(b):
    m = (b / 1024) / 1024
    return m


# This is a loop that repeats every monitoring_period to collect statistics
while True:
    # Call utilities with one secong delay that output general_disk_io_usage, general_cpu_load, client_cpu_load, client_memory_usage percent
    iostat_call = subprocess.Popen(
        ["iostat", "-d", "-t", "-y", "-o", "JSON", f"{monitoring_period}", "1"], stdout=subprocess.PIPE
    )
    mpstat_call = subprocess.Popen(["mpstat", "-o", "JSON", f"{monitoring_period}", "1"], stdout=subprocess.PIPE)

    # Read and decode outputs
    iostat_call_output = iostat_call.stdout.read().decode("utf8")
    mpstat_call_output = mpstat_call.stdout.read().decode("utf8")

    total_cpu_times = get_total_cpu_times()

    # Get a dic whith net_usage_metrics
    net_usage_output = get_net_usage()

    # Get total count of memory usage
    memory_call_output = psutil.virtual_memory()

    # Because iostat and mpstat outputs have JSON format there are use json.loads for read it
    iostat_json_output = json.loads(iostat_call_output)
    mpstat_json_output = json.loads(mpstat_call_output)

    # Get general_disk_io_usage and timestamp from iostat output
    vda_io_usage_wrtn = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["disk"][-3]["kB_wrtn"]
    vda_io_usage_read = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["disk"][-3]["kB_read"]
    iostat_timestamp = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["timestamp"]

    # Make monitoring filename from iostat timestamp
    file_name = iostat_timestamp.replace(" ", "-").replace(":", "-")

    # Try calculating disk_io_usage for rubackup_client and all childs processes
    # Sometimes, if the child process no longer exists, then execution of psutil.Process(childs_pid).io_counters() may fail

    before = datetime.now()
    client_stats = calculate_client_total_stat(get_all_child_process())
    print("times" + str(datetime.now() - before))

    # Get general_cpu_load
    cpu_load_usr = mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["cpu-load"][0]["usr"]
    mpstat_timestamp = mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["timestamp"]

    # Get memory usage metrics and convert to megabytes
    total_memory = memory_call_output.total
    available_memory = memory_call_output.available
    general_memory_usage_percent = memory_call_output.percent
    general_memory_usage = total_memory - available_memory
    general_memory_usage_m = b_to_m(general_memory_usage)
    # client_memory_usage_percent = pidstat_call_output.split("\n")[3].split()[13]
    # print(client_memory_usage_percent)
    # client_memory_usage = (total_memory / 100) * 0.23
    # client_memory_usage_m = b_to_m(client_memory_usage)
    # client_memory_percent = psutil.Process(pid).memory_percent()
    psutil_cpu_percent = psutil.cpu_percent()
    current_time = str(datetime.now())

    # The collected metrics are added to the dictionary where first level keys it is a formatted outup of "date" utility, which also is a name of monitoring file.
    statistics[file_name] = {
        "disk_io_usage": {
            "current_time": current_time,
            "iostat_timestamp": iostat_timestamp,
            "vda_io_usage_wrtn": vda_io_usage_wrtn,
            "vda_io_usage_read": vda_io_usage_read,
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
            "client_memory_usage_percent": 0,
            "client_memory_usage_m": 0,
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
        # statistics.pop(list(statistics.keys())[0])

    # Get a first, second key name in dictionary and path to RuBackup monitoring files
    key_name = list(statistics.keys())[0]
    key_name_next = list(statistics.keys())[1]
    file_path = monitoring_files_path + key_name

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

    # Appendind calculated client_disk_io_usage and net_usage to dictionary
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
            print("iostat_timestamp" + " " + statistics[key_name]["disk_io_usage"]["iostat_timestamp"])
            print("mpstat_timestamp" + " " + statistics[key_name]["cpu_usage"]["mpstat_timestamp"])
            # print("current_time" + " " + statistics[key_name]["disk_io_usage"]["current_time"])
            print("\n")
            print("psutil_cpu_percent" + " " + str(statistics[key_name]["cpu_usage"]["psutil_cpu_percent"]))
            print("general_cpu_usage" + " " + monitoring_file_data["general_cpu_usage"])
            print("client_cpu_usage" + " " + monitoring_file_data["client_cpu_usage"])
            print("mpstat_general_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["cpu_load_usr"]))
            print("test_client_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["client_cpu_load_percent"]))
            print("\n")
            print("general_io_usage_r" + " " + monitoring_file_data["general_io_usage_r"])
            print("iostat_general_io_usage_r" + " " + str(statistics[key_name]["disk_io_usage"]["vda_io_usage_read"]))
            print("client_io_usage_r" + " " + monitoring_file_data["client_io_usage_r"])
            print("client_io_usage_read" + " " + str(statistics[key_name]["disk_io_usage"]["client_io_usage_read_KB"]))
            print("client_io_usage_w" + " " + monitoring_file_data["client_io_usage_w"])
            print(
                "client_io_usage_write" + " " + str(statistics[key_name]["disk_io_usage"]["client_io_usage_write_KB"])
            )
            print("general_io_usage_w" + " " + monitoring_file_data["general_io_usage_w"])
            print("iostat_general_io_usage_w" + " " + str(statistics[key_name]["disk_io_usage"]["vda_io_usage_wrtn"]))
            print("\n")
            print("general_net_usage_w" + " " + monitoring_file_data["general_net_usage_w"])
            print("net_sent_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_sent_KB"]))
            print("general_net_usage_r" + " " + monitoring_file_data["general_net_usage_r"])
            print("net_recieved_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_recieved_KB"]))
            print("\n")
            print("general_ram_usage_%" + " " + monitoring_file_data["general_ram_usage"])
            print(
                "general_memory_usage_%"
                + " "
                + str(statistics[key_name]["memory_usage"]["general_memory_usage_percent"])
            )
            print("client_ram_usage_%" + " " + monitoring_file_data["client_ram_usage"])
            print(
                "client_memory_usage_%"
                + " "
                + str(statistics[key_name]["memory_usage"]["client_memory_usage_percent"])
            )
            print("general_ram_usage_m" + " " + monitoring_file_data["general_ram_usage_m"])
            print("general_memory_usage_m" + " " + str(statistics[key_name]["memory_usage"]["general_memory_usage_m"]))
            print("client_ram_usage_m" + " " + monitoring_file_data["client_ram_usage_m"])
            print("client_memory_usage_m" + " " + str(statistics[key_name]["memory_usage"]["client_memory_usage_m"]))
            print("\n")

            # Deleting first record in dictionary
            statistics.pop(list(statistics.keys())[0])
            print("------------------")
    except IOError as e:
        statistics.pop(list(statistics.keys())[0])
        print(e)
        print("------------------")
        continue
