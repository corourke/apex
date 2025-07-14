#!/bin/bash

# Check if a filename argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <python_file>"
  exit 1
fi

python_file="$1"

export RAY_ADDRESS="http://127.0.0.1:8265"
ray job submit --working-dir . --runtime-env-json='{"pip": ["hudi", "ray[data,train]", "xgboost", "lightgbm"]}' -- python "$python_file"
