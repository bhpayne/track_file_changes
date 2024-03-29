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


current       | current | previous      | previous |
file path     | hash    | file path     | hash     | status 
-------------------------------------------------------------------
/path/to/file | abcd    | /path/to/file | abcd     | no change, no move
/pgin/gin/gi  | gingia  | /pgin/gin/gi  | ingna    | file changed
/p/t/f        | 1234    | /a/b/c        | 1234     | file moved
/pa/to/fi     | 992d    | NaN           | NaN      | file added
NaN           | NaN     | /ag/ggn/g9jgn | 9g9248   | file deleted

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
        elif '--path_to_ouput' in arg:
            path_to_output = arg.replace('--path_to_output=', '')
        elif '--email' in arg:
            email_addr = arg.replace('--email=', '')
    if not os.path.exists(path_to_json):
        raise Exception('ERROR: provided json path does not exist:', path_to_json)
    if not os.path.exists(path_to_output):
        raise Exception('ERROR: provided output path does not exist:', path_to_output)
    return prnt_debug, path_to_json, path_to_output, email_addr

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
        print('--path_to_output="/path/to/write"')
        print('--email="my_name@domain.com"')
        print('optional argument:')
        print('--debug')
        sys.exit(1) # https://stackoverflow.com/questions/6501121/difference-between-exit-and-sys-exit-in-python
    elif len(list_of_args) > 1:
        prnt_debug, path_to_json, path_to_output, email_addr = parse_args(list_of_args)
    else:
        raise Exception('invalid option')
    return prnt_debug, path_to_json, path_to_output, email_addr

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
    del dict_of_times[latest_json_file]
    second_latest_json_file = max(dict_of_times.keys(), key=(lambda k: dict_of_times[k]))
    if prnt_debug: print('latest:', latest_json_file)
    if prnt_debug: print('second latest:', second_latest_json_file)
    return latest_json_file, second_latest_json_file


def df_comparison_changed_files(prnt_debug, df_previous, df_current):
    """
    >>> df_previous = pandas.DataFrame(['/adfm/gasg', 'gmig9jiga'],
    ...                                columns=('full path', 'hash of file'))
    >>> df_current  = pandas.DataFrame(['/mg/gadfgsg', 'imginag'],
    ...                                columns=('full path', 'hash of file'))
    >>> df_comparison_changed_files(False, df_previous, df_current)

    """
    list_of_changed_hashes = []
    df_changed_hash = pandas.merge(df_current, df_previous,
                                   on='full path', how='outer', indicator=True,
                                   suffixes=['_current', '_previous'])
    df_both = df_changed_hash[df_changed_hash['_merge'] == 'both']
    df_altered = df_both[df_both['hash of file_current'] != df_both['hash of file_previous']]

    if df_altered.shape[0] > 0:
        print('changed files:')
        for index, row in df_altered.iterrows():
            print(row['full path'])

            df_current = df_current[df_current['full path'] != row['full path']]
            df_previous = df_previous[df_previous['full path'] != row['full path']]

#            list_of_changed_hashes.append(row['hash of file_current'])
#            list_of_changed_hashes.append(row['hash of file_previous'])
    return df_previous, df_current

def df_comparison_moved_files(prnt_debug, df_previous, df_current):
    """
    file moved, aka renamed:    same hash,    changed path
     
    >>> df_comparison_moved_files(False, ) 
    """
    df_merged_hash = pandas.merge(df_current, df_previous, 
                                  on='hash of file', how='outer', indicator=True, 
                                  suffixes=['_current', '_previous'])
    df_both = df_merged_hash[df_merged_hash['_merge'] == 'both']

    df_moved = df_both[df_both['full path_current'] != df_both['full path_previous']]

    list_of_moved_hashes = []
    if df_moved.shape[0] > 0:
        print("== moved files ==")
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_moved.iterrows():
            print(row['full path_previous'], '-->', row['full path_current'])

            df_current = df_current[df_current['full path'] != row['full path_current']]
            df_previous = df_previous[df_previous['full path'] != row['full path_previous']]

#            list_of_moved_hashes.append(row['hash of file'])
    return df_previous, df_current

def df_comparison_new_and_deleted_files(prnt_debug, df_previous, df_current):
    """
    # find files that were added and files that were removed
    # file added:    new hash,       new path
    # file removed:  missing hash, missing path

    >>> df_comparison_new_and_deleted_files(False)
    """
    # https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html
    # only merge rows when all three columns match
    df_merged_all = pandas.merge(df_current, df_previous, 
                                 on=['hash of file', 'full path'], 
                                 how='outer', indicator=True, 
                                 suffixes=['_current', '_previous'])

    df_new_files     = df_merged_all[df_merged_all['_merge'] == 'left_only']
    df_deleted_files = df_merged_all[df_merged_all['_merge'] == 'right_only']

    if df_new_files.shape[0] > 0:
        print('== new files ==')
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_new_files.iterrows():
            print(row['full path'])

    if df_deleted_files.shape[0] > 0:
        print('== deleted files ==')
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_deleted_files.iterrows():
            print(row['full path'])
    return

my_str = """
import diff_changes as dc
import pandas
import glob
prnt_debug = False
path_to_json = '.' 
path_to_output = '.'
list_of_json_files = glob.glob(path_to_json+'/*.json')
latest_json_file, second_latest_json_file = dc.get_latest_json(prnt_debug, list_of_json_files)
df_previous = pandas.read_json(second_latest_json_file)
df_current = pandas.read_json(latest_json_file)
"""

if __name__ == '__main__':

    prnt_debug, path_to_json, path_to_output, email_addr  = args_use(sys.argv)
    list_of_json_files = glob.glob(path_to_json+'/*.json')

    if len(list_of_json_files) > 1:
        latest_json_file, second_latest_json_file = get_latest_json(prnt_debug, list_of_json_files)   
    else:
        print("need at least two previous JSON files. Exiting.")
        sys.exit(0)

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html 
    df_previous = pandas.read_json(second_latest_json_file)
    df_current = pandas.read_json(latest_json_file)


    #print('== detecting changes ==')
    # options:
    # file added:    new hash,     new path
    # file removed:  missing hash, missing path

    # file moved, aka renamed:    same hash,     new path
    # file changed:  changed hash (new hash and missing hash for same same path)

    # https://stackoverflow.com/questions/9758450/pandas-convert-dataframe-to-array-of-tuples/9762084
    current_tuples = [tuple(x) for x in df_current.values]
    previous_tuples = [tuple(x) for x in df_previous.values]

    unchanged_tuples = set(current_tuples).intersection(set(previous_tuples))
    # remove unchanged_tuples from both current_tuples and previous_tuples
    df_current_to_cat  = pandas.DataFrame(list(set(current_tuples)  - unchanged_tuples), columns=['full path','hash of file'])
    df_previous_to_cat = pandas.DataFrame(list(set(previous_tuples) - unchanged_tuples), columns=['full path','hash of file'])
#    print('after removing "no change" files, current df is',df_current_to_cat.shape)
#    print('after removing "no change" files, prev    df is',df_previous_to_cat.shape)

    df_previous_to_cat, df_current_to_cat = df_comparison_changed_files(prnt_debug, df_previous_to_cat, df_current_to_cat)
#    print('after detecting changes, current df is',df_current_to_cat.shape)
#    print('after detecting changes, prev    df is',df_previous_to_cat.shape)

    df_previous_to_cat, df_current_to_cat = df_comparison_moved_files(prnt_debug, df_previous_to_cat, df_current_to_cat)
#    print('after moved files, current df is',df_current_to_cat.shape)
#    print('after moved files, prev    df is',df_previous_to_cat.shape)

    df_comparison_new_and_deleted_files(prnt_debug, df_previous_to_cat, df_current_to_cat)

