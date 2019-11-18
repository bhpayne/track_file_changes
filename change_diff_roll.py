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
import change_tracker
import diff_changes
import rolling_logs

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
    if not os.path.exists(path_to_output):
        raise Exception('ERROR: provided output path does not exist:', path_to_output)
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


if __name__ == '__main__':

    prnt_debug, path_to_json, number_to_keep  = args_use(sys.argv)


