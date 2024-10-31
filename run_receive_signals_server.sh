#!/bin/bash

# Set timezone at the start of the script
export TZ=America/New_York

echo "=== Job started at $(date) ==="
echo "New York time: $(TZ=America/New_York date)"
echo "UTC time: $(TZ=UTC date)"
echo "Current directory: $(pwd)"
echo "User: $(whoami)"
echo "PATH: $PATH"

# Initialize conda
eval "$(/root/anaconda3/condabin/conda shell.bash hook)"

# Activate the conda environment
conda activate myenv

# Run the Python script
python3 -m mining.run_receive_signals_server

# Capture the exit status
STATUS=$?

# Deactivate conda environment
conda deactivate

echo "Exit status: $STATUS"
echo "=== Job finished at $(date) ==="