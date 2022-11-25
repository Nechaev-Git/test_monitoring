#!/usr/bin/env python3

import subprocess
import json
import psutil

statistics = {}

monitoring_files_path = "/opt/rubackup/monitoring/rubackup-stress.rubackup.local_db7963975bdae884/"

pid_and_childs_pids = []

for proc in psutil.process_iter():
    if "rubackup_client" in proc.name():
        pid = proc.pid
        pid_and_childs_pids.append(pid)


def get_all_child_process():
    for children in psutil.Process(pid).children(recursive=True):
        pid_and_childs_pids.append(children.pid)
    return pid_and_childs_pids


def get_io_for_all_childs(list_of_child_process):
    child_io_read_bytes = 0
    child_io_write_bytes = 0

    for childs_pid in list_of_child_process:
        child_io_read_bytes += psutil.Process(childs_pid).io_counters().read_bytes
        child_io_write_bytes += psutil.Process(childs_pid).io_counters().write_bytes

    child_total_io_read_KB = child_io_read_bytes / 1024
    child_total_io_write_KB = child_io_write_bytes / 1024

    return {
        "child_total_io_read_KB": int(child_total_io_read_KB),
        "child_total_io_write_KB": int(child_total_io_write_KB),
    }


def net_usage(inf="ens18"):  # change the inf variable according to the interface
    net_stat = psutil.net_io_counters(pernic=True, nowrap=True)[inf]
    net_in = net_stat.bytes_recv
    net_out = net_stat.bytes_sent

    with open(f"/proc/{pid}/net/dev") as net_dev_file:
        net_dev_stats = net_dev_file.read()
        net_client_recieve_bytes = net_dev_stats.split()[21]
        net_client_transmit_bytes = net_dev_stats.split()[28]

    net_usage_stats = {
        "net_recieved": net_in,
        "net_sent": net_out,
        "net_client_usage_w": int(net_client_transmit_bytes),
        "net_client_usage_r": int(net_client_recieve_bytes),
    }

    return net_usage_stats


def b_to_m(b):
    m = (b / 1024) / 1024
    return m


while True:

    iostat_call = subprocess.Popen(["iostat", "-d", "-t", "-y", "-o", "JSON", "1", "1"], stdout=subprocess.PIPE)
    mpstat_call = subprocess.Popen(["mpstat", "-o", "JSON", "1", "1"], stdout=subprocess.PIPE)
    pidstat_call = subprocess.Popen(
        ["pidstat", "-h", "-u", "-r", "-I", "-p", f"{pid}", "1", "1"], stdout=subprocess.PIPE
    )
    memory_output = psutil.virtual_memory()

    iostat_call_output = iostat_call.stdout.read().decode("utf8")
    mpstat_call_output = mpstat_call.stdout.read().decode("utf8")
    pidstat_call_output = pidstat_call.stdout.read().decode("utf8")
    memory_call_output = psutil.virtual_memory()
    print(pidstat_call_output)
    iostat_json_output = json.loads(iostat_call_output)
    mpstat_json_output = json.loads(mpstat_call_output)

    vda_io_usage_wrtn = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["disk"][-1]["kB_wrtn"]
    vda_io_usage_read = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["disk"][-1]["kB_read"]
    iostat_timestamp = iostat_json_output["sysstat"]["hosts"][0]["statistics"][0]["timestamp"]

    io_client_stats = get_io_for_all_childs(get_all_child_process())

    cpu_load_usr = mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["cpu-load"][0]["usr"]
    client_cpu_load_usr = pidstat_call_output.split("\n")[3].split()[8]
    mpstat_timestamp = mpstat_json_output["sysstat"]["hosts"][0]["statistics"][0]["timestamp"]

    total_memory = memory_call_output.total
    available_memory = memory_call_output.available
    general_memory_usage_percent = memory_call_output.percent
    general_memory_usage = total_memory - available_memory
    general_memory_usage_m = b_to_m(general_memory_usage)
    client_memory_usage_percent = pidstat_call_output.split("\n")[3].split()[14]
    client_memory_usage = (total_memory / 100) * float(client_memory_usage_percent)
    client_memory_usage_m = b_to_m(client_memory_usage)

    net_usage_output = net_usage()

    current_time = subprocess.getoutput("date +%a\ %b\ %e\ %T\ %G")
    file_name = subprocess.getoutput("date +%F-%k-%M-%S")

    statistics[file_name] = {
        "disk_io_usage": {
            "iostat_timestamp": iostat_timestamp,
            "vda_io_usage_wrtn": vda_io_usage_wrtn,
            "vda_io_usage_read": vda_io_usage_read,
            "client_io_usage_total": io_client_stats,
            "client_io_usage_read_KB": 0,
            "client_io_usage_write_KB": 0,
        },
        "net_usage_total": net_usage_output,
        "net_usage_rates": {
            "net_recieved_KB": 0,
            "net_sent_KB": 0,
            "net_client_recieved_KB": 0,
            "net_client_sent_KB": 0,
        },
        "cpu_usage": {
            "mpstat_timestamp": mpstat_timestamp,
            "cpu_load_usr": cpu_load_usr,
            "client_cpu_load_usr": client_cpu_load_usr,
        },
        "memory_usage": {
            "general_memory_usage_m": general_memory_usage_m,
            "general_memory_usage_percent": general_memory_usage_percent,
            "client_memory_usage_percent": client_memory_usage_percent,
            "client_memory_usage_m": client_memory_usage_m,
        },
    }

    if len(statistics) < 6:
        del pid_and_childs_pids[1:]
        print(pid_and_childs_pids)
        continue
    elif len(statistics) > 6:
        del pid_and_childs_pids[1:]
        statistics.pop(list(statistics.keys())[0])

    key_name = list(statistics.keys())[0]
    key_name_next = list(statistics.keys())[1]
    file_path = monitoring_files_path + key_name

    io_client_read = (
        statistics[key_name_next]["disk_io_usage"]["client_io_usage_total"]["child_total_io_read_KB"]
        - statistics[key_name]["disk_io_usage"]["client_io_usage_total"]["child_total_io_read_KB"]
    )
    io_client_write = (
        statistics[key_name_next]["disk_io_usage"]["client_io_usage_total"]["child_total_io_write_KB"]
        - statistics[key_name]["disk_io_usage"]["client_io_usage_total"]["child_total_io_write_KB"]
    )

    next_net_rates_in = (
        statistics[key_name_next]["net_usage_total"]["net_recieved"]
        - statistics[key_name]["net_usage_total"]["net_recieved"]
    ) / 1024
    next_net_rates_out = (
        statistics[key_name_next]["net_usage_total"]["net_sent"] - statistics[key_name]["net_usage_total"]["net_sent"]
    ) / 1024

    next_net_client_rates_in = (
        statistics[key_name_next]["net_usage_total"]["net_client_usage_r"]
        - statistics[key_name]["net_usage_total"]["net_client_usage_r"]
    ) / 1024
    next_net_client_rates_out = (
        statistics[key_name_next]["net_usage_total"]["net_client_usage_w"]
        - statistics[key_name]["net_usage_total"]["net_client_usage_w"]
    ) / 1024

    statistics[key_name_next]["disk_io_usage"] = {
        "client_io_usage_read_KB": io_client_read,
        "client_io_usage_write_KB": io_client_write,
    }
    print(statistics[key_name_next]["disk_io_usage"]["client_io_usage_read_KB"])
    statistics[key_name_next]["net_usage_rates"] = {
        "net_recieved_KB": next_net_rates_in,
        "net_sent_KB": next_net_rates_out,
        "net_client_recieved_KB": next_net_client_rates_in,
        "net_client_sent_KB": next_net_client_rates_out,
    }

    try:
        with open(file_path, "r") as j:
            monitoring_file_data = json.load(j)
            print("timestamp_before" + " " + monitoring_file_data["timestamp_before"])
            print("timestamp_after" + " " + monitoring_file_data["timestamp_after"])
            print("\n")
            print("general_cpu_usage" + " " + monitoring_file_data["general_cpu_usage"])
            print("client_cpu_usage" + " " + monitoring_file_data["client_cpu_usage"])
            print("mpstat_general_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["cpu_load_usr"]))
            print("pidstat_client_cpu_load" + " " + str(statistics[key_name]["cpu_usage"]["client_cpu_load_usr"]))
            print("\n")
            print("general_io_usage_r" + " " + monitoring_file_data["general_io_usage_r"])
            print("iostat_general_io_usage_r" + " " + str(statistics[key_name]["disk_io_usage"]["vda_io_usage_read"]))
            print("client_io_usage_r" + " " + monitoring_file_data["client_io_usage_r"])
            print("client_io_usage_w" + " " + monitoring_file_data["client_io_usage_w"])
            print("general_io_usage_w" + " " + monitoring_file_data["general_io_usage_w"])
            print("iostat_general_io_usage_w" + " " + str(statistics[key_name]["disk_io_usage"]["vda_io_usage_wrtn"]))
            print("\n")
            print("general_net_usage_w" + " " + monitoring_file_data["general_net_usage_w"])
            print("net_sent_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_sent_KB"]))
            print("client_net_usage_w" + " " + monitoring_file_data["client_net_usage_w"])
            print("net_client_sent_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_client_sent_KB"]))
            print("general_net_usage_r" + " " + monitoring_file_data["general_net_usage_r"])
            print("net_recieved_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_recieved_KB"]))
            print("client_net_usage_r" + " " + monitoring_file_data["client_net_usage_r"])
            print(
                "net_client_recieved_KB" + " " + str(statistics[key_name]["net_usage_rates"]["net_client_recieved_KB"])
            )
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
            print("iostat_timestamp" + " " + statistics[key_name]["disk_io_usage"]["iostat_timestamp"])
            print("mpstat_timestamp" + " " + statistics[key_name]["cpu_usage"]["mpstat_timestamp"])

            del pid_and_childs_pids[1:]
            print(pid_and_childs_pids)

            print("------------------")
    except IOError as e:
        del pid_and_childs_pids[1:]
        print(pid_and_childs_pids)
        print(e)
        print("------------------")
        continue
