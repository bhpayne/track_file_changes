#!/usr/bin/env python

"""
Ben Payne
20191101

standard use:
python3 rolling_logs.py --path_to_json="/home/jovyan/tmp" --number_to_keep="4"

python3 rolling_logs.py --path_to_json="/home/jovyan/tmp" --debug

********************
# https://docs.python.org/3/library/profile.html
python -m cProfile -s time change_tracker.py --path="/home/jovyan/tmp" | head -n 100

********************
# https://pycallgraph.readthedocs.io/en/develop/guide/command_line_usage.html
pip install pycallgraph
pycallgraph graphviz -- ./change_tracker.py  --path="/home/jovyan/tmp"

*********************
pip install pylint
python3 -m pylint change_tracker.py

*********************
# https://docs.python.org/3/library/doctest.html
# https://www.python.org/dev/peps/pep-0257/
python3 -m doctest change_tracker.py

************************

"""

import datetime # for JSON file name
import glob
import sys
import os
import hashlib # hash of file
import pandas
import change_tracker as ct

def parse_args(list_of_args):
    """
    >>> parse_args(['name of py script', '--path_to_json="/root"', '--debug'])

    >>> parse_args(['name of py script', '--path_to_json="/root"'])

    >>> parse_args(['name of py script', 'invalid arg'])
    """
    prnt_debug = False
    path_to_json = '.'
    number_to_keep = '3'
    email_addr = 'none'
    for arg in list_of_args:
        if '--debug' in arg:
            prnt_debug = True
        elif '--path_to_json' in arg:
            path_to_json = arg.replace('--path_to_json=', '')
        elif '--number_to_keep' in arg:
            number_to_keep = arg.replace('--number_to_keep=', '')
    try:
        number_to_keep = int(number_to_keep)
    except:
        print('unable to convert')
    if number_to_keep<1:
        raise Exception('ERROR: number to keep must be greater than 0')
    if not os.path.exists(path_to_json):
        raise Exception('ERROR: provided json path does not exist:', path_to_json)
    return prnt_debug, path_to_json, number_to_keep

def args_use(list_of_args):
    """
    >>> args_use(['name of file'])

    >>> args_use(['name_of_py', '--debug'])
    """
    prnt_debug = False
    path_to_search = ''
    if len(list_of_args) == 1:
        print('invalid number of arguments')
        print('required argument:')
        print('--path_to_json="/path/to/search"')
        print('--number_to_keep="3"')
        print('optional argument:')
        print('--debug')
        sys.exit(1) # https://stackoverflow.com/questions/6501121/difference-between-exit-and-sys-exit-in-python
    elif len(list_of_args) > 1:
        prnt_debug, path_to_json, number_to_keep = parse_args(list_of_args)
    else:
        raise Exception('invalid option')
    return prnt_debug, path_to_json, number_to_keep

def identify_logs_to_delete(prnt_debug, list_of_json_files, number_to_keep):
    """
    >>> identify_logs_to_delete(False, ['file1.json', 'file2.json', 'file3.json', 2]

    """
    import string
    dict_of_times = {}
    for json_file in list_of_json_files:
        # https://stackoverflow.com/questions/1450897/remove-characters-except-digits-from-string-using-python
        date_to_parse = json_file.replace('.json', '').replace('_', '').translate(str.maketrans('', '', string.ascii_letters)).replace('.','').replace('/','')
        dict_of_times[json_file] = datetime.datetime.strptime(date_to_parse, "%Y-%m-%d%H-%M")
    # https://www.w3resource.com/python-exercises/dictionary/python-data-type-dictionary-exercise-15.php

    list_of_files_to_delete = []
    while len(dict_of_times)>0:
        latest_json_file = max(dict_of_times.keys(), key=(lambda k: dict_of_times[k]))
        if len(dict_of_times)>number_to_keep+1:
            if prnt_debug: print('keeping',latest_json_file)
            pass
        else:
            list_of_files_to_delete.append(latest_json_file)
            if prnt_debug: print('to eliminate:',latest_json_file)
        del dict_of_times[latest_json_file]

    return list_of_files_to_delete

def delete_old_logs(prnt_debug, path_to_json, number_to_keep):
    """
    """
    list_of_json_files = glob.glob(path_to_json+'/*.json')

    if len(list_of_json_files) > 1:
        list_of_files = identify_logs_to_delete(prnt_debug, list_of_json_files, number_to_keep) 
    else:
        print("need at least two previous JSON files. Exiting.")
        sys.exit(0)

    for this_file in list_of_files:
        try:
            os.remove(this_file)
        except:
            raise Exception("unable to delete file")
    return

if __name__ == '__main__':

    prnt_debug, path_to_json, number_to_keep  = args_use(sys.argv)

    delete_old_logs(prnt_debug, path_to_json, number_to_keep)

