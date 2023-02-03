#!/usr/bin/env python3

from statistics import mean
import numpy as np
import sys
import matplotlib.pyplot as plt

statistics = {}


def parse_stats():
    with open("/home/u/test_monitoring/statistics5min") as stats:
        list_stats = stats.readlines()
    for line in list_stats:
        parsed_line = line.replace("\n", "").split()
        if "timestamp_after" in line and "timestamp_after" not in statistics.keys():
            statistics["timestamp_after"] = [parsed_line[-1]]
        elif "timestamp_after" in line:
            statistics["timestamp_after"].append(parsed_line[-1])
        elif len(parsed_line) >= 2:
            if parsed_line[0] not in statistics.keys():
                statistics[parsed_line[0]] = [
                    parsed_line[1],
                ]
            else:
                statistics[parsed_line[0]].append(parsed_line[1])


def make_plot(monitoring_parameter):
    fig = plt.figure(figsize=(35, 10))
    plt.grid()
    plt.yticks(fontsize=6)
    plt.xticks(rotation=90)
    if "cpu" in monitoring_parameter:
        y = np.arange(1, 101)
        plt.ylim(0, 100)
        plt.yticks(y, y)
    timestamp_line = np.array(statistics["timestamp_after"][int(sys.argv[1]) : int(sys.argv[2])])
    parameter_names = []
    for key in statistics.keys():
        if monitoring_parameter in key:
            parameter_names.append(key)
    for name in parameter_names:
        locals()[name] = np.array([float(i) for i in statistics[name]][int(sys.argv[1]) : int(sys.argv[2])])
        plt.plot(timestamp_line, locals()[name], label=name)
        print(f"Average {name} " + str(mean(locals()[name])))
    plt.legend()
    plt.show()


parse_stats()
make_plot("net_r")
