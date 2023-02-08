#!/usr/bin/env python3

import subprocess
import psutil
from collections import defaultdict
import platform
import sys
from datetime import datetime
from time import sleep

# Сетевой интерфейс и диск для которых будет собираться статистика
net_interface = "ens18"
disk = "sda"

# Период мониторинга. Передаётся аргументом к скрипту
monitoring_period = sys.argv[1]

# Из имени хоста и полученого hwid составляется путь до директории с файлами мониторинга
hostname = platform.node()
hwid = subprocess.getoutput("/opt/rubackup/bin/rubackup_client hwid").split("\n")[2]
monitoring_files_path = "/opt/rubackup/monitoring/" + hostname + "_" + hwid + "/"

# Создание словарей для последующего размещения в них собранной статистики.
# Словари с постфиксом "_counters" используются для собираемой ежесекундно статистики, из которой будет считаться
# дельта между показателями за текущую секунду и предыдущую.
# Дельта будет использована при расчёте процентов для некоторых показателей и для перевода в другие еденицы измерения.
# После расчёта процентов и перевода дельты в необходимые единицы измерения, полученные значения помещаются в словари
# general_stats и client_stats.
general_stats_counters = {}
client_stats_counters = {}
general_stats = {}
client_stats = {}

# Поиск pid'а для процесса rubackup_client. Создаё список, потому что потом добавим в него все дочерние процессы
pid_and_childs_pids = [proc.pid for proc in psutil.process_iter() if "rubackup_client" in proc.name()]

# Функция для сбора и подсчёта общесистемной статистики.
def collect_general_stats(timestamp):
    # cpu_times
    cpu_times = psutil.cpu_times()
    total_cpu_time = sum(vals for vals in cpu_times if vals != "guest" and vals != "guest_nice")
    total_use_cpu_time = total_cpu_time - (cpu_times.idle + cpu_times.iowait)
    # net_usage
    net_counter = psutil.net_io_counters(pernic=True, nowrap=True)[net_interface]
    net_in_bytes = net_counter.bytes_recv / 1024
    net_out_bytes = net_counter.bytes_sent / 1024
    # io
    io_counter = psutil.disk_io_counters(perdisk=True, nowrap=True)
    io_read_Kb = io_counter[disk].read_bytes / 1024
    io_write_Kb = io_counter[disk].write_bytes / 1024
    # memory
    memory_stats = psutil.virtual_memory()
    total_memory = memory_stats.total
    available_memory = memory_stats.available
    memory_usage_percent = memory_stats.percent
    memory_usage_m = (total_memory - available_memory) / (1024 * 1024)

    general_stats_counters[timestamp] = {
        "total_cpu_time": total_cpu_time,
        "total_use_cpu_time": total_use_cpu_time,
        "net_in": net_in_bytes,
        "net_out": net_out_bytes,
        "io_read": io_read_Kb,
        "io_write": io_write_Kb,
        "memory_usage_percent": memory_usage_percent,
        "memory_usage_m": memory_usage_m,
    }
    # Вызывается функция для расчёта показателей, если длина словаря > 2.
    if len(general_stats_counters) >= 2:
        calculate_general_stats(timestamp, general_stats_counters, general_stats)


# Функция для расчёта статистики по каждому показателю
def calculate_general_stats(timestamp, general_stats_counters, general_stats):
    total_cpu_time = general_stats_counters[timestamp]["total_cpu_time"]
    total_use_cpu_time = general_stats_counters[timestamp]["total_use_cpu_time"]
    net_in = general_stats_counters[timestamp]["net_in"]
    net_out = general_stats_counters[timestamp]["net_out"]
    io_read = general_stats_counters[timestamp]["io_read"]
    io_write = general_stats_counters[timestamp]["io_write"]

    # Для получение показателей из предыдущей итерации получаем значение предыдущего ключа.
    prev_timestamp = list(general_stats_counters.keys())[-2]

    # Расчёт дельты между показателями из текущей итерации и предыдущей
    delta_total_cpu_time = total_cpu_time - general_stats_counters[prev_timestamp]["total_cpu_time"]
    delta_total_use_cpu_time = total_use_cpu_time - general_stats_counters[prev_timestamp]["total_use_cpu_time"]
    delta_net_in = net_in - general_stats_counters[prev_timestamp]["net_in"]
    delta_net_out = net_out - general_stats_counters[prev_timestamp]["net_out"]
    delta_io_read = io_read - general_stats_counters[prev_timestamp]["io_read"]
    delta_io_write = io_write - general_stats_counters[prev_timestamp]["io_write"]

    cpu_usage_percent = (delta_total_use_cpu_time / delta_total_cpu_time) * 100
    memory_usage_percent = general_stats_counters[timestamp]["memory_usage_percent"]
    memory_usage_m = general_stats_counters[timestamp]["memory_usage_m"]

    general_stats[timestamp] = {}
    general_stats[timestamp]["cpu_percent"] = cpu_usage_percent
    general_stats[timestamp]["net_in"] = delta_net_in
    general_stats[timestamp]["net_out"] = delta_net_out
    general_stats[timestamp]["io_read"] = delta_io_read
    general_stats[timestamp]["io_write"] = delta_io_write
    general_stats[timestamp]["memory_usage_percent"] = memory_usage_percent
    general_stats[timestamp]["memory_usage_m"] = memory_usage_m


def get_clients_stats(proc_pid):
    try:
        child_proc = psutil.Process(proc_pid)
        with child_proc.oneshot():
            client_times_user = child_proc.cpu_times().user
            client_times_system = child_proc.cpu_times().system
            child_io_read_KB = child_proc.io_counters().read_bytes / 1024
            child_io_write_KB = child_proc.io_counters().write_bytes / 1024
            child_memory_m = child_proc.memory_info().rss / (1024 * 1024)

            return [client_times_user + client_times_system, child_io_read_KB, child_io_write_KB, child_memory_m]
    except:
        print(f"File /proc/{proc_pid}/stat doesn't exist")
        return [0, 0, 0, 0]


def calculate_client_total_stat(list_of_child_pids, timestamp):

    list_of_stat = map(get_clients_stats, list_of_child_pids)
    list_of_stat_zipped = zip(*list_of_stat)
    # total_list = [sum(i) for i in list_of_stat_zipped]
    total_list = list(map(sum, list_of_stat_zipped))

    client_stats_counters[timestamp] = {
        "client_cpu_time": total_list[0],
        "client_io_read": total_list[1],
        "client_io_write": total_list[2],
        "client_memory": total_list[3],
    }


def collect_client_stats(timestamp):
    rubackup_client_pid = pid_and_childs_pids[0]

    for children in psutil.Process(rubackup_client_pid).children(recursive=True):
        pid_and_childs_pids.append(children.pid)

    calculate_client_total_stat(pid_and_childs_pids, timestamp)

    del pid_and_childs_pids[1:]

    if len(client_stats_counters) >= 2:
        total_cpu_time = general_stats_counters[timestamp]["total_cpu_time"]
        client_total_cpu_time = client_stats_counters[timestamp]["client_cpu_time"]
        client_io_read = client_stats_counters[timestamp]["client_io_read"]
        client_io_write = client_stats_counters[timestamp]["client_io_write"]
        total_memory_m = general_stats[timestamp]["memory_usage_m"]

        prev_timestamp = list(client_stats_counters.keys())[-2]
        delta_total_cpu_time = total_cpu_time - general_stats_counters[prev_timestamp]["total_cpu_time"]
        delta_client_cpu_time = client_total_cpu_time - client_stats_counters[prev_timestamp]["client_cpu_time"]
        delta_client_io_read = client_io_read - client_stats_counters[prev_timestamp]["client_io_read"]
        delta_client_io_write = client_io_write - client_stats_counters[prev_timestamp]["client_io_write"]

        client_cpu_usage_percent = (delta_client_cpu_time / delta_total_cpu_time) * 100
        client_memory_m = client_stats_counters[timestamp]["client_memory"]
        client_memory_percent = (client_memory_m / total_memory_m) * 100

        client_stats[timestamp] = {}
        client_stats[timestamp]["client_cpu_percent"] = client_cpu_usage_percent
        client_stats[timestamp]["client_io_read"] = delta_client_io_read
        client_stats[timestamp]["client_io_write"] = delta_client_io_write
        client_stats[timestamp]["client_memory_percent"] = client_memory_percent
        client_stats[timestamp]["client_memory_m"] = client_memory_m


def collect_stats():
    timestamp = str(datetime.now().strftime("%Y-%m-%d-%H-%M-%S"))
    collect_general_stats(timestamp)
    collect_client_stats(timestamp)


while True:
    collect_stats()
    print("client_stats " + str(client_stats) + "\n")
    print("general_stats " + str(general_stats) + "\n")
    sleep(1)
