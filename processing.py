#!/usr/bin/env python2.7

import pandas as pd
from factors import *
from csv import DictReader, DictWriter
from distribution import *



def main():
    file_execution_time = "function_durations_percentiles.anon.d01.csv"
    file_memory_utlization = "app_memory_percentiles.anon.d01.csv"
    df_memory=pd.read_csv(file_memory_utlization)

    functions = []
    fn_count = 0

    memo_memory = {}
    memo_non_memory = []
    


    with open(file_execution_time, 'r') as csvfile:
        reader = DictReader(csvfile)
        for row in reader:
            print("processing row ",fn_count)
            # skip the first row
            if fn_count == 0:
                fn_count += 1
            else:
                function_info = {}

                '''
                Extracting following function info from Azure Trace file:
                HashOwner
                HashApp
                HashFunction
                ExecutionTime
                '''
                function_info["HashOwner"] = row["HashOwner"]
                function_info["HashApp"] = row["HashApp"]
                function_info["HashFunction"] = row["HashFunction"]
                function_info["ExecutionTime"] = row["percentile_Average_50"]

                '''
                Extracting following function info from Azure Trace file:
                Memory Utlization
                '''
                if function_info["HashApp"] in memo_non_memory:
                    continue
                if function_info["HashApp"] in memo_memory:
                    mem_util = memo_memory[function_info["HashApp"]]
                else:
                    df_memory_utlization = df_memory.query('HashApp=="{}"'.format(function_info["HashApp"]))
                    if not df_memory_utlization.empty:
                        mem_util = df_memory_utlization['AverageAllocatedMb_pct50'].iloc[0]
                        memo_memory[function_info["HashApp"]] = mem_util
                    else:
                        memo_non_memory.append(function_info["HashApp"])
                        continue
               
                function_info["MemoryUse"] = mem_util

                '''
                Calculate Cold Start Time based on Memory Utlization:
                Memory Utlization
                '''
                function_info["ColdStartTime"] = cold_start_from_memory(mem_util)
                functions.append(function_info)
                fn_count += 1
    
    
    num_functions = fn_count-1
    data_cpu_utlization = cpu_utlization(num_functions)

    '''
    Add CPU utlization for each funciton
    '''
    for i in range(num_functions):
        functions[i]["CPUUtilization"] = data_cpu_utlization[i]

    '''
    Write trace file
    '''
    outfilename = "trace.csv"
    with open(outfilename, 'w') as outfile:
        fieldnames = ['HashOwner', 'HashApp', 'HashFunction','ExecutionTime', 'ColdStartTime', 'MemoryUse', 'CPUUtilization']
        writer = DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for fn in functions:
            writer.writerow(fn)


if __name__ == "__main__":
    main()
