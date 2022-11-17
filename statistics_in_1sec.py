#!/usr/bin/env python3

import subprocess
import json
import psutil

statistics = {}

monitoring_files_path = "/opt/rubackup/monitoring/rubackup-stress.rubackup.local_db7963975bdae884/"

def net_usage(inf = "ens18"):   #change the inf variable according to the interface
    
    net_stat = psutil.net_io_counters(pernic=True, nowrap=True)[inf]
    net_in = net_stat.bytes_recv
    net_out = net_stat.bytes_sent

    net_usage_stats = {'net_recieved': net_in, 'net_sent': net_out}

    return net_usage_stats

while True:
    iostat_call = subprocess.Popen(['iostat','-d','-t','-y','-o','JSON','1','1'], stdout=subprocess.PIPE)
    mpstat_call = subprocess.Popen(['mpstat','-o','JSON','1','1'], stdout=subprocess.PIPE)
    pidstat_call = subprocess.Popen(['pidstat','-h','-u','-C','rubackup_client','1','1'], stdout=subprocess.PIPE)

    iostat_call_output = iostat_call.stdout.read().decode('utf8')
    mpstat_call_output = mpstat_call.stdout.read().decode('utf8')
    pidstat_call_output = pidstat_call.stdout.read().decode('utf8')
    #print(pidstat_call_output)

    iostat_json_output = json.loads(iostat_call_output)
    mpstat_json_output = json.loads(mpstat_call_output)

    vda_io_usage_wrtn = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_wrtn']
    vda_io_usage_read = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_read']
    iostat_timestamp = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['timestamp']
    
    cpu_load_usr = mpstat_json_output['sysstat']['hosts'][0]['statistics'][0]['cpu-load'][0]['usr']
    client_cpu_load_usr = pidstat_call_output.split('\n')[3].split()[4]
    mpstat_timestamp = mpstat_json_output['sysstat']['hosts'][0]['statistics'][0]['timestamp']

    net_usage_output = net_usage()

    current_time = subprocess.getoutput('date +%a\ %b\ %e\ %T\ %G')
    file_name = subprocess.getoutput('date +%F-%k-%M-%S')
    
    statistics[file_name] = {'disk_io_usage': {'iostat_timestamp': iostat_timestamp, 'vda_io_usage_wrtn': vda_io_usage_wrtn, 'vda_io_usage_read': vda_io_usage_read}, \
                             'net_usage_total': net_usage_output, \
                             'net_usage_rates': {'net_recieved_KB': 0,'net_sent_KB': 0},\
                             'cpu_usage': {'mpstat_timestamp': mpstat_timestamp,'cpu_load_usr': cpu_load_usr, 'client_cpu_load_usr': client_cpu_load_usr}, \
                            }
    #print(iostat_timestamp)
    #print(net_usage_output)
    #print(statistics)

    if len(statistics) < 5:
        continue
    elif len(statistics) > 5:
        statistics.pop(list(statistics.keys())[0])
    
    key_name = list(statistics.keys())[0]
    key_name_next = list(statistics.keys())[1]
    file_path = monitoring_files_path + key_name

    next_net_rates_in = (statistics[key_name_next]['net_usage_total']['net_recieved'] - statistics[key_name]['net_usage_total']['net_recieved']) / 1024
    next_net_rates_out = (statistics[key_name_next]['net_usage_total']['net_sent'] - statistics[key_name]['net_usage_total']['net_sent']) / 1024

    statistics[key_name_next]['net_usage_rates'] = {'net_recieved_KB': next_net_rates_in, 'net_sent_KB': next_net_rates_out}

    try:
        with open(file_path,'r') as j:
            monitoring_file_data = json.load(j)
            print('timestamp_before'+' '+monitoring_file_data['timestamp_before'])
            print('timestamp_after'+' '+monitoring_file_data['timestamp_after'])
            print('\n')
            print('general_cpu_usage'+' '+monitoring_file_data['general_cpu_usage'])
            print('client_cpu_usage'+' '+monitoring_file_data['client_cpu_usage'])
            print('mpstat_general_cpu_load'+' '+str(statistics[key_name]['cpu_usage']['cpu_load_usr']))
            print('pidstat_client_cpu_load'+' '+str(statistics[key_name]['cpu_usage']['client_cpu_load_usr']))
            print('\n')
            print('general_io_usage_r'+' '+monitoring_file_data['general_io_usage_r'])
            print('iostat_general_io_usage_r'+' '+str(statistics[key_name]['disk_io_usage']['vda_io_usage_read']))
            print('client_io_usage_r'+' '+monitoring_file_data['client_io_usage_r'])
            print('general_io_usage_w'+' '+monitoring_file_data['general_io_usage_w'])
            print('iostat_general_io_usage_w'+' '+str(statistics[key_name]['disk_io_usage']['vda_io_usage_wrtn']))
            print('\n')
            print('general_net_usage_w'+' '+monitoring_file_data['general_net_usage_w'])
            print('net_sent_KB'+' '+str(statistics[key_name]['net_usage_rates']['net_sent_KB']))
            print('client_net_usage_w'+' '+monitoring_file_data['client_net_usage_w'])
            print('general_net_usage_r'+' '+monitoring_file_data['general_net_usage_r'])
            print('net_recieved_KB'+' '+str(statistics[key_name]['net_usage_rates']['net_recieved_KB']))
            print('client_net_usage_r'+' '+monitoring_file_data['client_net_usage_r'])
            print('\n')
            print('iostat_timestamp'+' '+statistics[key_name]['disk_io_usage']['iostat_timestamp'])
            print('mpstat_timestamp'+' '+statistics[key_name]['cpu_usage']['mpstat_timestamp'])

            #print(current_time)
            print('------------------')
    except IOError as e:
        print(e)
        print('------------------')
        continue