#!/usr/bin/env python

"""
Ben Payne
20191101

standard use:
python3 diff_changes.py --path_to_json="/home/jovyan/tmp"

python3 diff_changes.py --path_to_json="/home/jovyan/tmp" --debug

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

input dataframes:

file path     | hash    | status    | details
-------------------------------------------------
/path/to/file | a852b9f | see below | see below

status can be 
* duplicate
* changed
* moved
* deleted
* added

Sequence of analysis:
1) duplicate
2) changed
3) moved
4) added or deleted

If status==changed, then detail is old hash

If status==moved, then detail is old path


"""

import datetime # for JSON file name
import glob
import sys
import os
import hashlib # hash of file
import pandas
#import change_tracker as ct
import numpy as np

def parse_args(list_of_args):
    """
    >>> parse_args(['name of py script', '--path_to_json="/root"', '--debug'])

    >>> parse_args(['name of py script', '--path_to_json="/root"'])

    >>> parse_args(['name of py script', 'invalid arg'])
    """
    prnt_debug = False
    path_to_json = '.'
    path_to_output = '.'
    email_addr = 'none'
    for arg in list_of_args:
        if '--debug' in arg:
            prnt_debug = True
        elif '--path_to_json' in arg:
            path_to_json = arg.replace('--path_to_json=', '')
    if not os.path.exists(path_to_json):
        raise Exception('ERROR: provided json path does not exist:', path_to_json)
    return prnt_debug, path_to_json

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
        print('optional argument:')
        print('--debug')
        sys.exit(1) # https://stackoverflow.com/questions/6501121/difference-between-exit-and-sys-exit-in-python
    elif len(list_of_args) > 1:
        prnt_debug, path_to_json = parse_args(list_of_args)
    else:
        raise Exception('invalid option')
    return prnt_debug, path_to_json

def get_latest_json(prnt_debug, list_of_json):
    """
    which json file is the latest and which json file is the second latest?
    >>> get_latest_json(False, ['file1.json', 'file2.json'])
    """
    import string
    dict_of_times = {}
    for json_file in list_of_json:
        # https://stackoverflow.com/questions/1450897/remove-characters-except-digits-from-string-using-python
        date_to_parse = json_file.replace('.json', '').replace('_', '').replace('/','').replace('.','').translate(str.maketrans('', '', string.ascii_letters))
        dict_of_times[json_file] = datetime.datetime.strptime(date_to_parse, "%Y-%m-%d%H-%M")
    # https://www.w3resource.com/python-exercises/dictionary/python-data-type-dictionary-exercise-15.php
    latest_json_file = max(dict_of_times.keys(), key=(lambda k: dict_of_times[k]))
    return latest_json_file


def find_duplicate_files(prnt_debug, df):
    """
    this is for a single crawl of the directory -- no comparison with previous JSON records needed

    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.duplicated.html

    >>> find_duplicate_files(False, [{'hash of file':'asdfmagin'}])
    """
    dupe_str = ""
    df_dupes = df[df.duplicated(subset='hash of file', keep=False)]
    if df_dupes.shape[0] > 0:
        dupe_str += '== duplicate files (based on hash) ==\n'
        for this_hash in set(df_dupes['hash of file'].values):
            dupe_str += 'hash: ' + this_hash + '\n'
            path_series = df_dupes[df_dupes['hash of file']==this_hash]['full path']
            for idx, this_path in path_series.items():
                dupe_str += this_path + '\n'
                df.at[idx,'status'] = 'duplicate'
            # the following overwrites other instances
            #df['status'] = np.where(df['hash of file'] == this_hash, 'duplicate', '')

    #df_no_dupes = df[df['status']!='duplicate']
    return dupe_str


if __name__ == '__main__':

    prnt_debug, path_to_json = args_use(sys.argv)

    list_of_json = glob.glob(path_to_json+'/*.json')
    #print(list_of_json)

    latest_json = get_latest_json(prnt_debug, list_of_json)
    #print(latest_json)

    df = pandas.read_json(latest_json)

    dupe_str = find_duplicate_files(prnt_debug, df)

    print(dupe_str)
