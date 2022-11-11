#!/usr/bin/env python3

import subprocess
import json

statistics = {}

monitoring_files_path = "/opt/rubackup/monitoring/rubackup-stress.rubackup.local_db7963975bdae884/"

while True:
    iostat_call = subprocess.Popen(['iostat','-d','-t','-y','-o','JSON','1','1'], stdout=subprocess.PIPE)
    iostat_call_output = iostat_call.stdout.read().decode('utf8')
    iostat_json_output = json.loads(iostat_call_output)
    vda_io_usage_wrtn = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_wrtn']
    vda_io_usage_read = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_read']
    iostat_timestamp = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['timestamp']
    
    current_time = subprocess.getoutput('date +%a\ %b\ %e\ %T\ %G')
    file_name = subprocess.getoutput('date +%F-%k-%M-%S')
      
    statistics[file_name] = {'vda_io_usage_wrtn': vda_io_usage_wrtn, 'vda_io_usage_read': vda_io_usage_read}
    
    if len(statistics) < 60:
        continue
    elif len(statistics) > 60:
        statistics.pop(list(statistics.keys())[0])
    
    key_name = list(statistics.keys())[0]
    file_path = monitoring_files_path + key_name

    try:
        with open(file_path,'r') as j:
            monitoring_file_data = json.load(j)
            print('timestamp_before'+' '+monitoring_file_data['timestamp_before'])
            print('timestamp_after'+' '+monitoring_file_data['timestamp_after'])
            print('general_io_usage_r'+' '+monitoring_file_data['general_io_usage_r'])
            print('iostat_general_io_usage_r'+' '+str(statistics[key_name]['vda_io_usage_read']))
            print('client_io_usage_r'+' '+monitoring_file_data['client_io_usage_r'])
            print('general_io_usage_w'+' '+monitoring_file_data['general_io_usage_w'])
            print('iostat_general_io_usage_w'+' '+str(statistics[key_name]['vda_io_usage_wrtn']))

            print(current_time)
            print('------------------')
    except IOError as e:
        print(e)
        print('------------------')
        continue