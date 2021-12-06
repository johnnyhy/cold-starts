import pandas as pd
from factors import *
from csv import DictReader, DictWriter
from distribution import *
import configparser
import os
import sys
import numpy as np





def main(nameDirPairs1,nameDirPairs2):

    if len(nameDirPairs1) != len(nameDirPairs2):
        sys.exit("config error, check the file config.ini")
    num_pairs = len(nameDirPairs1)
    
    for i in range(num_pairs):
        cur_file_number = i+1
        print(f"producing trace file number {cur_file_number}")

        file_execution_time = nameDirPairs1[i][1]
        file_memory_utlization = nameDirPairs2[i][1]
        df_memory=pd.read_csv(file_memory_utlization)

        functions = []
        row_count = 0

        memo_memory = {}
        memo_non_memory = []
        
        data_execution_time = []
        data_memory_use = []

        with open(file_execution_time, 'r') as csvfile:
            reader = DictReader(csvfile)
            for row in reader:
            
                # skip the first row
                if row_count == 0:
                    row_count += 1
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
                    function_info["ExecutionTime"] = "{:.2f}".format(float(row["percentile_Average_50"]))
                    data_execution_time.append(float(function_info["ExecutionTime"]))

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
                
                    function_info["MemoryUse"] = "{:.2f}".format(float(mem_util))
                    data_memory_use.append(float(function_info["MemoryUse"]))

                    functions.append(function_info)
                    row_count += 1
        
        
        num_functions = row_count-1

        '''
        Add Cold Start Time 
        '''

        # data_cpu_utlization = exp_distribution(16, num_functions)
        data_cs_time = exp_distribution(5110.12, num_functions)

        for i in range(num_functions):
            functions[i]["ColdStartTime"] = "{:.2f}".format(data_cs_time[i])
            # functions[i]["CPUUtilization"] = "{:.2f}".format(data_cpu_utlization[i])
            
        '''
        Add timestamp for each funciton
        '''

        for i in range(num_functions):
            if i == 0:
                functions[i]["TimeStamp"] = 0
            else:
                functions[i]["TimeStamp"] = functions[i-1]["TimeStamp"] + float(functions[i-1]["ExecutionTime"])


        '''
        Write trace file
        '''
        out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'trace')

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)


        outfilename = os.path.join(out_dir, f'trace{cur_file_number}.csv')

        arr_data_execution_time = np.array(data_execution_time)
        arr_data_memory_use = np.array(data_memory_use)

        with open(outfilename, 'w') as outfile:
            # write comments 
            # stat info of Execution Time
            mean = "{:.2f}".format(arr_data_execution_time.mean())
            median = "{:.2f}".format(np.median(arr_data_execution_time))
            min = "{:.2f}".format(arr_data_execution_time.min())
            max = "{:.2f}".format(arr_data_execution_time.max())
            std = "{:.2f}".format(arr_data_execution_time.std())
            outfile.write(f"# Execution Time: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")
            # stat info of Cold Start Time
            mean = "{:.2f}".format(data_cs_time.mean())
            median = "{:.2f}".format(np.median(data_cs_time))
            min = "{:.2f}".format(data_cs_time.min())
            max = "{:.2f}".format(data_cs_time.max())
            std = "{:.2f}".format(data_cs_time.std())
            outfile.write(f"# Cold Start Time: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")

            # stat info of Memory Use
            mean = "{:.2f}".format(arr_data_memory_use.mean())
            median = "{:.2f}".format(np.median(arr_data_memory_use))
            min = "{:.2f}".format(arr_data_memory_use.min())
            max = "{:.2f}".format(arr_data_memory_use.max())
            std = "{:.2f}".format(arr_data_memory_use.std())
            outfile.write(f"# Memory Utlization: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")

            # # stat info of CPU Use
            # mean = "{:.2f}".format(data_cpu_utlization.mean())
            # median = "{:.2f}".format(np.median(data_cpu_utlization))
            # min = "{:.2f}".format(data_cpu_utlization.min())
            # max = "{:.2f}".format(data_cpu_utlization.max())
            # std = "{:.2f}".format(data_cpu_utlization.std())
            # outfile.write(f"# CPU Utlization: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")

            # write data
            fieldnames = ['TimeStamp', 'HashOwner', 'HashApp', 'HashFunction','ExecutionTime', 'ColdStartTime', 'MemoryUse']
            writer = DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for fn in functions:
                writer.writerow(fn)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    config.read(os.path.join(PROJECT_PATH, 'config.ini'))
    main(config.items('execution_time_paths'),config.items('memory_utlization_paths'))
