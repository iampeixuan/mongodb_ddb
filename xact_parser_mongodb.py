import os
import sys
import json
import glob
import datetime
import numpy as np
import pymongo
from functions.function_N import xact_N
from functions.function_P import xact_P
from functions.function_D import xact_D
from functions.function_O import xact_O
from functions.function_S import xact_S
from functions.function_I import xact_I
from functions.function_T import xact_T
from functions.function_R import xact_R
from functions.function_NULL import xact_NULL


# parse user input and call the corresponding function, return the xact type and time elapsed
def parse_xact(database, xact, func_dict):
    if len(xact) > 1:
        func_type = xact[0][0]
    else:
        func_type = xact[0]
    return func_dict[func_type](database, xact)


# pre-defined one-to-one mapping dictionary
# TODO: Replace transaction once implemented
xact_dict = {
    'N': xact_N,
    'P': xact_P,
    'D': xact_D,
    'O': xact_O,
    'S': xact_S,
    'I': xact_I,
    'T': xact_T,
    'R': xact_R
}

# dictionary to record latency for each type of xact
# TODO: Remove placeholder 0 and NULL key
latency_record = {
    'N': [],
    'P': [],
    'D': [],
    'O': [],
    'S': [],
    'I': [],
    'T': [],
    'R': [],
}

if __name__ == '__main__':
    # get the user input arguments
    args = sys.argv[1:]
    if len(args) != 3:
        print("please give exact arguments: [IP] [database_name] [xact file id]")
        print("terminating program...")
        exit(1)
    IP, database_name, file_id = args
    if IP == 'local':
        IP = '127.0.0.1'

    # test connection and work on collection
    try:
        client = pymongo.MongoClient(IP, serverSelectionTimeoutMS=1)
        print(client.server_info())
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        print("terminating program...")
        exit(1)

    # set read/write consistency and connect to database
    r = pymongo.read_concern.ReadConcern("majority")
    w = pymongo.write_concern.WriteConcern("majority", wtimeout=30000)
    mongo_client = pymongo.MongoClient(IP, 27017, serverSelectionTimeoutMS=300000)
    db = mongo_client.get_database(database_name, write_concern=w, read_concern=r)
    print("working on database : ", db.name)

    # get the xact files, sorted by the number in file name
    xact_dir = r"./DB-project-files/xact-files"
    xact_files = sorted(glob.glob(xact_dir + os.sep + "*.txt"), key=lambda s: int(os.path.basename(s)[:-4]))

    # read all the lines from the specified file
    xact_file = xact_files[int(file_id)-1]
    print("running xact parser on: " + xact_file)
    with open(xact_file) as f:
        xact_lines = f.readlines()
        total_num_lines = len(xact_lines)
    
    # iterate through every line and call parser function
    line_index = 0
    xact_counter = 0
    t_start = datetime.datetime.now()
    while line_index < total_num_lines:
        # only New Order Transaction has multiple lines (num_items)
        if xact_lines[line_index][0] == 'N':
            num_items = int(xact_lines[line_index].rstrip().split(',')[-1])
            x_type, lat = parse_xact(db, xact_lines[line_index: line_index + num_items + 1], xact_dict)
            line_index += (num_items + 1)
        # other 7 type of xact only one line 
        else:
            x_type, lat = parse_xact(db, xact_lines[line_index], xact_dict)
            line_index += 1

        # record the latency of each xact (based on xact type)
        latency_record[x_type].append(lat)
        xact_counter += 1
        print("progress: {}/{} lines \n".format(line_index, total_num_lines))

    # record the total time elapsed
    t_end = datetime.datetime.now()
    time_elapsed = (t_end - t_start).seconds

    # combine all the latency for statistics
    all_records = np.array(latency_record['N'] + latency_record['P'] + latency_record['D'] + latency_record['O'] + latency_record['S'] + latency_record['I'] + latency_record['T'] + latency_record['R'])

    # print to concole for output and saved to log file
    log_dir = "xact_parser_log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_name = log_dir + os.sep + "parser_log_{}.txt".format(file_id)
    if os.path.exists(log_file_name):
        print("removing old log file")
        os.remove(log_file_name)
    
    log_file = open(log_file_name, "w")
    log_file.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S \n"))
    
    outputs = ["Total number of xacts: {} \n".format(xact_counter), 
               "Total time elapsed (second): {} \n".format(time_elapsed),
               "Throughput (xact per second): {} \n".format(xact_counter/max(time_elapsed, 1)),
               "Average xact latency (ms): {} \n".format(all_records.mean()),
               "Median xact latency (ms): {} \n".format(np.percentile(all_records, 50)),
               "95th percentile xact latency (ms): {} \n".format(np.percentile(all_records, 95)),
               "99th percentile xact latency (ms): {} \n".format(np.percentile(all_records, 99)),
               "Average xact latency for each xact type (ms):  \n",
               "N: {} \n".format(np.array(latency_record['N']).mean()),
               "P: {} \n".format(np.array(latency_record['P']).mean()),
               "D: {} \n".format(np.array(latency_record['D']).mean()),
               "O: {} \n".format(np.array(latency_record['O']).mean()),
               "S: {} \n".format(np.array(latency_record['S']).mean()),
               "I: {} \n".format(np.array(latency_record['I']).mean()),
               "T: {} \n".format(np.array(latency_record['T']).mean()),
               "R: {} \n".format(np.array(latency_record['R']).mean()),
               "Number of xacts per type: {}, {}, {}, {}, {}, {}, {}, {} \n".format(
                       len(latency_record['N']), len(latency_record['P']), len(latency_record['D']), len(latency_record['O']),
                       len(latency_record['S']), len(latency_record['I']), len(latency_record['T']), len(latency_record['R']))]
    for l in outputs:
        print(l)
        log_file.write(l)
    
    log_file.close()
    print("saved to log: {}".format(log_file_name))


