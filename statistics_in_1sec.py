#!/usr/bin/env python3

import subprocess
import json

#statistics = {}

monitoring_files_path = "/opt/rubackup/monitoring/rubackup-stress.rubackup.local_db7963975bdae884/"

while True:
    iostat_call = subprocess.Popen(['iostat','-d','-t','-y','-o','JSON','1','1'], stdout=subprocess.PIPE)
    iostat_call_output = iostat_call.stdout.read().decode('utf8')
    iostat_json_output = json.loads(iostat_call_output)
    vda_io_usage_wrtn = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_wrtn']
    vda_io_usage_read = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][-1]['kB_read']
    iostat_timestamp = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['timestamp']
    
    #dm0_io_usage = iostat_json_output['sysstat']['hosts'][0]['statistics'][0]['disk'][0]['kB_wrtn']
    
    current_time = subprocess.getoutput('date +%a\ %b\ %e\ %T\ %G')
    new_file_name = subprocess.getoutput('date +%F-%k-%M-%S')
    
    new_file_path = monitoring_files_path + new_file_name
    print(new_file_path)
    
    #statistics[current_time] = vda_io_usage

    #if len(statistics) > 60:
        #statistics.pop(list(statistics)[0])
    
    try:
        with open(new_file_path,'r') as j:
            monitoring_file_data = json.load(j)
            print('timestamp_before'+' '+monitoring_file_data['timestamp_before'])
            print('timestamp_after'+' '+monitoring_file_data['timestamp_after'])
            print('general_io_usage_r'+' '+monitoring_file_data['general_io_usage_r'])
            print('iostat_general_io_usage_r'+' '+str(vda_io_usage_read))
            print('client_io_usage_r'+' '+monitoring_file_data['client_io_usage_r'])
            print('general_io_usage_w'+' '+monitoring_file_data['general_io_usage_w'])
            print('iostat_general_io_usage_w'+' '+str(vda_io_usage_wrtn))

            print(current_time)
            print('------------------')
    except IOError:
        print('no_file')
        print('------------------')
        continue
