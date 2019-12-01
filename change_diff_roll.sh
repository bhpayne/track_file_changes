#!/bin/bash

path_to_json="."

./change_tracker.py --search_path="./tmp" --write_path=${path_to_json} --output_prefix="logs"

./diff_changes.py --path_to_json=${path_to_json}  --path_to_output="."

./rolling_logs.py --path_to_json=${path_to_json}  --number_to_keep="3"

# email here

