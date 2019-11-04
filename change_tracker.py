#!/usr/bin/env python

"""
Ben Payne
20191101

standard use:
python3 change_tracker.py --path="/home/jovyan/tmp"

python3 change_tracker.py --path="/home/jovyan/tmp" --debug

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
    >>> parse_args(['name of py script', '--path="/root"', '--debug'])

    >>> parse_args(['name of py script', '--path="/root"'])

    >>> parse_args(['name of py script', 'invalid arg'])
    """
    prnt_debug = False
    path_to_search = ''
    for arg in list_of_args:
        if 'debug' in arg:
            prnt_debug = True
        elif 'path' in arg:
            path_to_search = arg.replace('--path=', '')
    if not os.path.exists(path_to_search):
        raise Exception('ERROR: provided path does not exist:', path_to_search)
    return prnt_debug, path_to_search

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
        print('--path="/path/to/search"')
        print('optional argument:')
        print('--debug')
        sys.exit(1) # https://stackoverflow.com/questions/6501121/difference-between-exit-and-sys-exit-in-python
    elif len(list_of_args) > 1:
        prnt_debug, path_to_search = parse_args(list_of_args)
    else:
        raise Exception('invalid option')
    return prnt_debug, path_to_search

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

def write_list_of_dicts_to_json_file(prnt_debug, list_of_dicts):
    """

    >>> write_list_of_dicts_to_json_file(False,[{'hash of file'}:'asmaing'])
    """
    df = pandas.DataFrame(list_of_dicts)
    # http://strftime.org/
    timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M")
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html
    file_name = 'snapshot_'+timestamp+'.json'
    df.to_json(file_name, orient='records')
    return file_name

def get_latest_json(prnt_debug, list_of_json):
    """
    which json file is the latest?
    """
    dict_of_times = {}
    for json_file in list_of_json:
        date_to_parse = json_file.replace('.json', '').replace('snapshot_', '')
        dict_of_times[json_file] = datetime.datetime.strptime(date_to_parse, "%Y-%m-%dT%H-%M")
    # https://www.w3resource.com/python-exercises/dictionary/python-data-type-dictionary-exercise-15.php
    latest_json_file = max(dict_of_times.keys(), key=(lambda k: dict_of_times[k]))
    if prnt_debug: print('latest:', latest_json_file)
    return latest_json_file

def find_duplicate_files(prnt_debug, list_of_dicts):
    """
    this is for a single crawl of the directory -- no comparison with previous JSON records needed

    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.duplicated.html

    >>> find_duplicate_files(False, [{'hash of file':'asdfmagin'}])
    """
    df = pandas.DataFrame(list_of_dicts)
    df_dupes = df[df.duplicated(subset='hash of file', keep=False)]
    if df_dupes.shape[0] > 0:
        print('duplicate files (based on hash)')
        print(df_dupes)
    return

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
            list_of_changed_hashes.append(row['hash of file_current'])
            list_of_changed_hashes.append(row['hash of file_previous'])
    return list_of_changed_hashes

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
        print("moved file:")
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_moved.iterrows():
            print(row['full path_previous'], '-->', row['full path_current'])
            list_of_moved_hashes.append(row['hash of file'])
    return list_of_moved_hashes

def df_comparison_new_and_deleted_files(prnt_debug, df_previous, df_current, list_of_moved_hashes):
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
        print('new files:')
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_new_files.iterrows():
            if row['hash of file'] not in list_of_moved_hashes:
                print(row['full path'])

    if df_deleted_files.shape[0] > 0:
        print('deleted files:')
        # https://stackoverflow.com/questions/16476924/how-to-iterate-over-rows-in-a-dataframe-in-pandas
        for index, row in df_deleted_files.iterrows():
            if row['hash of file'] not in list_of_moved_hashes:
                print(row['full path'])
    return

if __name__ == '__main__':

    list_of_json_files = glob.glob('*.json')
    prnt_debug, path_to_search = args_use(sys.argv)
    list_of_dicts = hash_list_of_files(prnt_debug, path_to_search)
    find_duplicate_files(prnt_debug, list_of_dicts)
    current_json_file = write_list_of_dicts_to_json_file(prnt_debug, list_of_dicts)
 
    if len(list_of_json_files) > 0:
        latest_json_file = get_latest_json(prnt_debug, list_of_json_files)   
    else:
        print("no previous JSON files exist. Exiting.")
        sys.exit(0)

    print('detecting changes')
    # options:
    # file added:    new hash,     new path
    # file removed:  missing hash, missing path

    # file moved, aka renamed:    same hash,     new path
    # file changed:  changed hash (new hash and missing hash for same same path)

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html 
    df_previous = pandas.read_json(latest_json_file)
    df_current = pandas.DataFrame(list_of_dicts)

    list_of_changed_hashes = df_comparison_changed_files(prnt_debug, df_previous, df_current)

    list_of_moved_hashes = df_comparison_moved_files(prnt_debug, df_previous, df_current) 

    hashes_to_ignore = list_of_changed_hashes + list_of_moved_hashes
    df_comparison_new_and_deleted_files(prnt_debug, df_previous, df_current, hashes_to_ignore)

