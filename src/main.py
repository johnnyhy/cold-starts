import pandas as pd
from factors import *
from csv import DictReader, DictWriter
from distribution import *
import configparser
import os
import sys
import numpy as np





def main(nameDirPairs1,nameDirPairs2,nameDirPairs3):

    if len(nameDirPairs1) != len(nameDirPairs2) != len(nameDirPairs3):
        sys.exit("config error, check the file config.ini")
    num_pairs = len(nameDirPairs1)
    
    for i in range(num_pairs):
        cur_file_number = i+1
        print(f"producing trace file number {cur_file_number}")

        file_execution_time = nameDirPairs1[i][1]
        file_memory_utlization = nameDirPairs2[i][1]
        file_function_invocation = nameDirPairs3[i][1]
        df_memory=pd.read_csv(file_memory_utlization)
        df_function_invocations = pd.read_csv(file_function_invocation)

        functions = []
        row_count = 0

        memo_memory = {}
        memo_non_memory = []
        
        data_execution_time = []
        data_memory_use = []
        data_invocation_counts = []

        with open(file_execution_time, 'r') as csvfile:
            reader = DictReader(csvfile)
            for row in reader:
                print(f"processing row {row_count}")

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
                    RunTime
                    '''
                    function_info["HashOwner"] = row["HashOwner"]
                    function_info["HashApp"] = row["HashApp"]
                    function_info["HashFunction"] = row["HashFunction"]
                    function_info["RunTime"] = "{:.2f}".format(float(row["percentile_Average_50"]))
                    data_execution_time.append(float(function_info["RunTime"]))

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
                
                    function_info["MemUse"] = "{:.2f}".format(float(mem_util))
                    data_memory_use.append(float(function_info["MemUse"]))


                    '''
                    Extracting following function info from Azure Trace file:
                    Function Invocation Counts
                    '''
                    df_func_invo = df_function_invocations.query('HashFunction=="{}"'.format(function_info["HashFunction"]))
                    if df_func_invo.empty:
                        continue
                    counts = df_func_invo.iloc[:,4:].sum(axis=1).values[0]
                    function_info["InvocationCounts"] = "{:.2f}".format(float(counts))
                    data_invocation_counts.append(float(function_info["InvocationCounts"]))

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
        Add Time for each funciton
        '''

        for i in range(num_functions):
            if i == 0:
                functions[i]["Time"] = 0
            else:
                functions[i]["Time"] = functions[i-1]["Time"] + float(functions[i-1]["RunTime"])


        '''
        Write trace file
        '''
        out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'trace')

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)


        outfilename = os.path.join(out_dir, f'trace{cur_file_number}.csv')

        arr_data_execution_time = np.array(data_execution_time)
        arr_data_memory_use = np.array(data_memory_use)
        arr_data_invocation_counts = np.array(data_invocation_counts)

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
            outfile.write(f"# Memory Utilization: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")

            # # stat info of Function Invocation Counts
            mean = "{:.2f}".format(arr_data_invocation_counts.mean())
            median = "{:.2f}".format(np.median(arr_data_invocation_counts))
            min = "{:.2f}".format(arr_data_invocation_counts.min())
            max = "{:.2f}".format(arr_data_invocation_counts.max())
            std = "{:.2f}".format(arr_data_invocation_counts.std())
            outfile.write(f"# Invocation Counts: Mean {mean}; Median {median}; Min {min}; Max {max}; Std Dev {std}\n")

            # write data
            fieldnames = ['Time', 'HashOwner', 'HashApp', 'HashFunction','RunTime', 'ColdStartTime', 'MemUse','InvocationCounts']
            writer = DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            for fn in functions:
                writer.writerow(fn)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    config.read(os.path.join(PROJECT_PATH, 'config.ini'))
    main(config.items('execution_time_paths'),config.items('memory_utlization_paths'),config.items('function_invocation_paths'))
