#!/usr/bin/env python

"""
Ben Payne
20191101

standard use:
python3 change_tracker.py --search_path="/home/jovyan/tmp" --write_path="."

python3 change_tracker.py --search_path="/home/jovyan/tmp" --write_path="." --debug


python3 -m json.tool < log_2019-11-10T16-19.json 

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

"""

import datetime # for JSON file name
import glob
import sys
import os
import hashlib # hash of file
import pandas

def parse_args(list_of_args):
    """
    >>> parse_args(['name of py script', '--search_path="/root"', '--write_path="/dir"', '--debug'])

    >>> parse_args(['name of py script', '--search_path="/root"', '--write_path="/dir"', '--output_prefix="myfile"'])

    >>> parse_args(['name of py script', 'invalid arg'])
    """
    prnt_debug = False
    path_to_search = '.'
    write_path = '.'
    output_prefix = 'log'
    for arg in list_of_args:
        if '--debug' in arg:
            prnt_debug = True
        elif '--search_path' in arg:
            path_to_search = arg.replace('--search_path=', '')
        elif '--write_path' in arg:
            write_path = arg.replace('--write_path=', '')
        elif '--output_prefix' in arg:
            output_prefix = arg.replace('--output_prefix=', '')
            
    if not os.path.exists(path_to_search):
        raise Exception('ERROR: provided search path does not exist:', path_to_search)
    if not os.path.exists(write_path):
        raise Exception('ERROR: provided write path does not exist:', write_path)
    return prnt_debug, path_to_search, write_path, output_prefix

def args_use(list_of_args):
    """
    >>> args_use(['name of file'])

    >>> args_use(['name_of_py', '--debug'])
    """
    prnt_debug = False
    path_to_search = '.'
    write_path = '.'
    output_prefix = 'log'
    if len(list_of_args) == 1:
        print('ERROR: invalid number of arguments')
        print('required arguments:')
        print('  --search_path="/path/to/search"')
        print('  --write_path="/path/to/write"')
        print('optional arguments:')
        print('  --output_prefix="logs"')
        print('  --debug')
        sys.exit(1) # https://stackoverflow.com/questions/6501121/difference-between-exit-and-sys-exit-in-python
    elif len(list_of_args) > 1:
        prnt_debug, path_to_search, write_path, output_prefix = parse_args(list_of_args)
    else:
        raise Exception('invalid option')
    return prnt_debug, path_to_search, write_path, output_prefix

def md5_file(fname):
    """
    >>> md5_file('')

    >>> md5_file()
    """
    try:
        with open(fname, "rb") as fil:
            file_content = fil.read()
        return True, hashlib.md5(file_content).hexdigest()
    except PermissionError:
        return False, ''

def hash_list_of_files(prnt_debug, path_to_search):
    list_of_dicts = []
    # https://stackoverflow.com/questions/2186525/use-a-glob-to-find-files-recursively-in-python
    # glob doesn't support hidden directories :(

    for filename in glob.iglob(path_to_search+'/**/*', recursive=True):
        if os.path.isfile(filename): 
            got_hash, hash_of_file = md5_file(filename)
            if got_hash:
                file_dict = {}
                file_dict['full path'] = filename
                file_dict['hash of file'] = hash_of_file
                if prnt_debug: print(filename, hash_of_file)
                list_of_dicts.append(file_dict)
    return list_of_dicts

def write_list_of_dicts_to_json_file(prnt_debug, list_of_dicts, write_path, output_prefix):
    """

    >>> write_list_of_dicts_to_json_file(False,[{'hash of file'}:'asmaing'], '/path/to/write', 'logs')
    """
    df = pandas.DataFrame(list_of_dicts)
    # http://strftime.org/
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
    file_name = output_prefix+'_'+timestamp+'.json'
    df.to_json(write_path+'/'+file_name, orient='records')
    return file_name


if __name__ == '__main__':

    prnt_debug, path_to_search, write_path, output_prefix = args_use(sys.argv)
    list_of_dicts = hash_list_of_files(prnt_debug, path_to_search)
    current_json_file = write_list_of_dicts_to_json_file(prnt_debug, list_of_dicts, write_path, output_prefix)
 

