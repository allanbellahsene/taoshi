#!/bin/bash

# Set timezone at the start of the script
export TZ=America/New_York

echo "=== Job started at $(date) ==="
echo "New York time: $(TZ=America/New_York date)"
echo "UTC time: $(TZ=UTC date)"
echo "Current directory: $(pwd)"
echo "User: $(whoami)"
echo "PATH: $PATH"

# Source conda base initialization
source /home/ubuntu/miniconda3/etc/profile.d/conda.sh

# Activate the conda environment
conda activate myenv

# Run the Python script
python3 -m mining.concretum_strategy.fetch_historical_data_daily

# Capture the exit status
STATUS=$?

# Deactivate conda environment
conda deactivate

echo "Exit status: $STATUS"
echo "=== Job finished at $(date) ==="