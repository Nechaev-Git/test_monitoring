#!/usr/bin/env python3

from subprocess import getoutput
import psutil
from collections import defaultdict
from platform import node
import sys
from os import listdir
from datetime import datetime
from time import sleep
from json import load as load
import re

# Период мониторинга. Передаётся аргументом к скрипту
monitoring_period = int(sys.argv[1])
# Необходимое количество записей
records = int(sys.argv[2])
# Количество итераций для необходимого количества записей
iterations = monitoring_period * records

print(f"{iterations} секунд необходимо для сбора статистики")

# Из имени хоста и полученого hwid составляется путь до директории с файлами мониторинга
hostname = node()
hwid = getoutput("/opt/rubackup/bin/rubackup_client hwid").split("\n")[2]
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
    net_counter = psutil.net_io_counters(nowrap=True)
    net_in_bytes = net_counter.bytes_recv / 1024
    net_out_bytes = net_counter.bytes_sent / 1024
    # io
    disk_name_pattern = re.compile(r"^[a-z]d[a-z]+$")
    io_counters = psutil.disk_io_counters(perdisk=True, nowrap=True)
    io_counters_block = {
        disk_name: value for disk_name, value in io_counters.items() if disk_name_pattern.match(disk_name)
    }
    io_read_Kb = sum(disk["read_bytes"] for disk in io_counters_block.values()) / 1024
    io_write_Kb = sum(disk["write_bytes"] for disk in io_counters_block.values()) / 1024
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
        # "cpu_percent": psutil.cpu_percent(),
    }
    # Вызывается функция для расчёта показателей, если длина словаря >= 2
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

    # Для получение показателей из предыдущей итерации получаем значение предыдущего ключа
    prev_timestamp = list(general_stats_counters.keys())[-2]

    # Расчёт дельты между показателями из текущей итерации и предыдущей
    delta_total_cpu_time = total_cpu_time - general_stats_counters[prev_timestamp]["total_cpu_time"]
    delta_total_use_cpu_time = total_use_cpu_time - general_stats_counters[prev_timestamp]["total_use_cpu_time"]
    delta_net_in = net_in - general_stats_counters[prev_timestamp]["net_in"]
    delta_net_out = net_out - general_stats_counters[prev_timestamp]["net_out"]
    delta_io_read = io_read - general_stats_counters[prev_timestamp]["io_read"]
    delta_io_write = io_write - general_stats_counters[prev_timestamp]["io_write"]

    # Расчёт показателей в процентах и мегабайтах
    # psutil_cpu_percent = general_stats_counters[timestamp]["cpu_percent"]
    cpu_usage_percent = (delta_total_use_cpu_time / delta_total_cpu_time) * 100
    memory_usage_percent = general_stats_counters[timestamp]["memory_usage_percent"]
    memory_usage_m = general_stats_counters[timestamp]["memory_usage_m"]

    # Добавление показателей в словарь general_stats
    general_stats[timestamp] = {}
    # general_stats[timestamp]["psutil_cpu_percent"] = psutil_cpu_percent
    general_stats[timestamp]["cpu_percent"] = cpu_usage_percent
    general_stats[timestamp]["net_in"] = delta_net_in
    general_stats[timestamp]["net_out"] = delta_net_out
    general_stats[timestamp]["io_read"] = delta_io_read
    general_stats[timestamp]["io_write"] = delta_io_write
    general_stats[timestamp]["memory_usage_percent"] = memory_usage_percent
    general_stats[timestamp]["memory_usage_m"] = memory_usage_m


# Функция используется внутри функции calculate_client_total_stat и считает статистику для одного процесса
# по всем показателям
def get_clients_stats(proc_pid, timestamp):
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
        print(f"{timestamp}")
        print(f"File /proc/{proc_pid}/stat doesn't exist")
        return [0, 0, 0, 0]


# Функция для расчёта суммы по каждому показателю для процесса rubackup_client и всех порожденных им процессов
def calculate_client_total_stat(list_of_child_pids, timestamp):

    # Функция map вызывает функцию get_clients_stats для каждого pid'а внутри списка list_of_child_pids
    # и возвращает итератор, состоящий из списков([cpu,io_read,io_write,memory])
    # со статистикой по каждому показателю для каждого pid'a
    list_of_stat = map(get_clients_stats, list_of_child_pids)
    # Функция zip составляет итератор, содержащий статистику, скомпонованную отдельно для каждого показателя
    list_of_stat_zipped = zip(*list_of_stat)
    # Функция map возвращает итератор, сотоящий из сумм значений каждого элемента в list_of_stat_zipped
    total_list = list(map(sum, list_of_stat_zipped))
    # Значения из списка total_list помещаются в соответствующее место в словаре
    client_stats_counters[timestamp] = {
        "client_cpu_time": total_list[0],
        "client_io_read": total_list[1],
        "client_io_write": total_list[2],
        "client_memory": total_list[3],
    }


def collect_client_stats(timestamp):
    rubackup_client_pid = pid_and_childs_pids[0]
    # Нахлждение всех процессов, порожденных процессом rubackup_client, и всех их дочерних процессов
    for children in psutil.Process(rubackup_client_pid).children(recursive=True):
        pid_and_childs_pids.append(children.pid)

    calculate_client_total_stat(pid_and_childs_pids, timestamp)
    # Удаляем идентификаторы всех дочерних процессов из списка
    del pid_and_childs_pids[1:]
    # Вызывается функция для расчёта показателей, если длина словаря >= 2
    if len(client_stats_counters) >= 2:
        total_cpu_time = general_stats_counters[timestamp]["total_cpu_time"]
        client_total_cpu_time = client_stats_counters[timestamp]["client_cpu_time"]
        client_io_read = client_stats_counters[timestamp]["client_io_read"]
        client_io_write = client_stats_counters[timestamp]["client_io_write"]
        total_memory_m = general_stats[timestamp]["memory_usage_m"]

        # Для получение показателей из предыдущей итерации получаем значение предыдущего ключа
        prev_timestamp = list(client_stats_counters.keys())[-2]
        # Расчёт дельты между показателями из текущей итерации и предыдущей
        delta_total_cpu_time = total_cpu_time - general_stats_counters[prev_timestamp]["total_cpu_time"]
        delta_client_cpu_time = client_total_cpu_time - client_stats_counters[prev_timestamp]["client_cpu_time"]
        delta_client_io_read = client_io_read - client_stats_counters[prev_timestamp]["client_io_read"]
        delta_client_io_write = client_io_write - client_stats_counters[prev_timestamp]["client_io_write"]
        # Расчёт показателей в процентах и мегабайтах
        client_cpu_usage_percent = (delta_client_cpu_time / delta_total_cpu_time) * 100
        client_memory_m = client_stats_counters[timestamp]["client_memory"]
        client_memory_percent = (client_memory_m / total_memory_m) * 100
        # Добавление показателей в словарь general_stats
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


# Подсчёт статистики за указанный период
def gather_period_stats():
    monitoring_files = listdir(monitoring_files_path)
    period_stats = {}
    keys_list = list(general_stats.keys())
    for key in keys_list:
        # Поиск ключей в списке со статистикой, которые совпадают с имеющимися файлами мониторинга
        if key in monitoring_files:
            end = keys_list.index(key) + 1
            # max 0 установлен, чтобы не полуичлся отрицательный индекс в тех случаях, когда
            # имя найденного файла совпадает с тем ключом в словаре, индекс которого < monitoring_period
            # Таким образом, значения первого ключа в словаре period_stats нужно игнорировать
            start = max(0, end - monitoring_period)
            values_general = [general_stats[k] for k in keys_list[start:end]]
            values_client = [client_stats[k] for k in keys_list[start:end]]
            period_stats[key] = {}
            # period_stats[key]["psutil_general_cpu_call"] = (
            #     sum([value["psutil_cpu_percent"] for value in values_general]) / monitoring_period
            # )
            period_stats[key]["psutil_general_cpu"] = (
                sum([value["cpu_percent"] for value in values_general]) / monitoring_period
            )
            period_stats[key]["psutil_general_net_usage_r"] = sum([value["net_in"] for value in values_general])
            period_stats[key]["psutil_general_net_usage_w"] = sum([value["net_out"] for value in values_general])
            period_stats[key]["psutil_general_io_usage_r"] = sum([value["io_read"] for value in values_general])
            period_stats[key]["psutil_general_io_usage_w"] = sum([value["io_write"] for value in values_general])
            period_stats[key]["psutil_general_ram_usage_%"] = general_stats[key]["memory_usage_percent"]
            period_stats[key]["psutil_general_ram_usage_m"] = general_stats[key]["memory_usage_m"]
            period_stats[key]["psutil_client_cpu"] = client_stats[key]["client_cpu_percent"]
            period_stats[key]["psutil_client_io_usage_r"] = sum([value["client_io_read"] for value in values_client])
            period_stats[key]["psutil_client_io_usage_w"] = sum([value["client_io_write"] for value in values_client])
            period_stats[key]["psutil_client_ram_usage_%"] = client_stats[key]["client_memory_percent"]
            period_stats[key]["psutil_client_ram_usage_m"] = client_stats[key]["client_memory_m"]

            get_monitoring_data(monitoring_files_path, key, period_stats)
    # Запись подсчитанной статистики за период в файл statistics
    with open("statistics", "w") as stat_file:
        for key_name, key_value in period_stats.items():
            stat_file.write(f"{key_name}\n")
            for stat_name, stat_value in key_value.items():
                stat_file.write(f"{stat_name} {stat_value}\n")
            stat_file.write(f"---------\n")


# Функция для парсинга и записи данных из файла мониторинга в общий словарь со ставтистикой за период
def get_monitoring_data(monitoring_files_path, key, period_stats):
    with open(monitoring_files_path + key, "r") as j:
        monitoring_file_data = load(j)
        for stat_name in monitoring_file_data:
            # Изменяем имена параметров general и client_ram_usage, чтобы не было совпадающих шаблонов в именах параметров
            # Для всех параметров из файла мониторинга добавляем префикс rb_, для последующей обработки при построении графика
            if stat_name == "general_ram_usage" or stat_name == "client_ram_usage":
                period_stats[key][f"rb_{stat_name}_%"] = monitoring_file_data[stat_name]
            else:
                period_stats[key][f"rb_{stat_name}"] = monitoring_file_data[stat_name]


while len(general_stats) < iterations:
    collect_stats()
    print(f"\rОсталось {iterations - len(general_stats)} секунд", end="")
    sleep(1)

gather_period_stats()
