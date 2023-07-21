import subprocess
import time
import os
import logging
import datetime
import re


# Function to check if 'company_overviews.py' was run today
def was_run_today(log_file):
    try:
        # Read the log file and check the last entry date
        with open(log_file, 'r') as f:
            lines = f.readlines()
            last_run_line = lines[-1]  # get the last line in the log file
            date_search = re.search(r"\d{4}-\d{2}-\d{2}", last_run_line)

            # Check if the date was found
            if date_search is None:
                return False

            last_run_date_str = date_search.group()
            last_run_date = datetime.datetime.strptime(last_run_date_str, "%Y-%m-%d").date()
            return last_run_date == datetime.date.today()
    except FileNotFoundError:
        return False

# Get the current script's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the log file
log_file_path = os.path.join(current_dir, 'script.log')

# Create a logger
logging.basicConfig(filename=log_file_path, level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Get the current script's absolute directory path
script_dir = os.path.dirname(os.path.abspath(__file__))

# List your scripts in the desired order of execution
scripts = [
    "alphavantagetickers.py",
    "company_overviews.py",
    "selected_pairs_history.py",
    "bracket_order.py"
]

# Function to run the script and handle errors with retries
def run_script_with_retries(script, max_retries=2, wait_time=20):
    retries = 0
    while retries <= max_retries:
        logging.info(f"Running {script}")

        # Construct the absolute path of the script
        script_path = os.path.join(script_dir, script)

        # Run the script and capture its exit code
        try:
            subprocess.check_call(["python3", script_path])
            break  # If the script is successful, break the loop
        except subprocess.CalledProcessError as e:
            logging.error(f"Error occurred while running {script}, exit code: {e.returncode}")
            retries += 1
            if retries <= max_retries:
                logging.info(f"Retrying {script} after waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logging.error(f"Script {script} failed after {max_retries} retries. Skipping this script and moving "
                              f"to the next one.")
                break

# Iterate over the scripts list and execute them one by one
for script in scripts:
    if script == "company_overviews.py" and was_run_today(log_file_path):
        logging.info(f"{script} was already run today. Skipping.")
        continue
    run_script_with_retries(script)