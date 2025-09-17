#!/usr/bin/env python3
"""
Simple wrapper to process phenology data
Usage: python3 process_phenology.py [input_directory]
If no directory is provided, defaults to ./Transskriptionen
"""

import subprocess
import sys
import os

# Check if required packages are installed
required_packages = ['odfpy', 'pandas']
for package in required_packages:
    try:
        __import__(package.replace('-', '_'))
    except ImportError:
        print(f"Installing required package: {package}")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package, '--user'])

# Run the main processor
if __name__ == "__main__":
    script_path = os.path.join(os.path.dirname(__file__), 'phenology_data_processor.py')
    subprocess.call([sys.executable, script_path] + sys.argv[1:])