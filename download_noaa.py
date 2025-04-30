import pandas as pd
import subprocess
import os
import sys
import requests
import gzip
import shutil
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


# Set the path for the crx2rnx executable. 
CRX2RNX_EXE_PATH = Path(__file__).with_name("CRX2RNX")

# Error handling: ensure you have the file with the right permissions.
assert CRX2RNX_EXE_PATH.is_file(), f"{CRX2RNX_EXE_PATH} not found. Did you copy the executable here?"
assert os.access(CRX2RNX_EXE_PATH, os.X_OK), f"{CRX2RNX_EXE_PATH} is not executable. Please set the permissions for this file"
    

def is_url_available(url, retries=3, delay=5):
    """Check whether a URL is available, with retries.

    I was getting errors when trying to download files, so this function
    tries the request multiple times and waits between attempts. The idea
    is to give the server a chance to reset if it's having issues.
    """
    for attempt in range(retries):
        try: 
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            print(f'Attempt {attempt + 1} failed for url: {url}')
            sleep(delay)
    return False


def remove_file(file_path: Path | str) -> bool:
    """Take in file path and remove it. Return True if removed and false otherwise."""
    try:
        os.remove(file_path)
        return True
    except Exception as e:
        print(f'Attempt to remove file failed for {file_path}: {e}')
        return False


def unzip_file(file_dir: str, gz_path: str) -> bool:
    """Unzip .gz file."""
    src = Path(file_dir) / gz_path   # convert str -> Path
    dst = src.with_suffix('') # Strip gz from file name
    try: 
        dst.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists
        with gzip.open(src, 'rb') as f_in:
            with open(dst, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return True
    except Exception as e:
        print(f'Unzip failed for {src}: {e}')
        return False


def handle_hatanaka_rinex(obs_file: str, data_dir: str) -> str:
    """Convert a Hatanaka-compressed RINEX .d file to .o format if needed.

    Args:
        obs_file: The filename of the observation file.
        data_dir: Directory where the file is located.

    Returns:
        The updated obs_file filename (converted to .o if needed).
    """

    src = Path(data_dir) / obs_file
    if not src.name.endswith("d"):
        return obs_file

    dst = src.with_suffix(src.suffix[:-1] + "o")
    if dst.exists():
        return dst.name

    try:
        subprocess.run(
            [str(CRX2RNX_EXE_PATH), str(src.resolve())],
            capture_output=True,
            text=True,
            check=True
        ) # Excecute the crx2rnx executable to convert d to o file. 

    except subprocess.CalledProcessError as e:
        print(f'crx2rnx failed with error: {e}')

    if not dst.exists(): 
        raise RuntimeError(f"{dst.name} was not created by crx2rnx")

    return dst.name


def get_combined_url(
    date: pd.DatetimeIndex, 
    station_id: str
) -> str:
    """Take in date and station_id and obtain download url."""
    download_year_full = date.year
    download_year_last_two = f"{date.year % 100:02d}"
    download_day = f"{date.dayofyear:03d}"

    # Command template: 
    # 'curl -O https://noaa-cors-pds.s3.amazonaws.com/rinex/2025/001/corv/corv0010.25d.gz'
    const_url = "https://noaa-cors-pds.s3.amazonaws.com/rinex"
    changing_url = f"{download_year_full}/{download_day}/{station_id}/{station_id}{download_day}0.{download_year_last_two}d.gz" # Use 'o' file type instead of 'd'. 
    combined_url = f"{const_url}/{changing_url}"

    return combined_url


def get_station_ids(
    station_id_filename: str="station_ids.csv", 
    station_id_column_name: str="SITEID"
) -> list[str]:
    """Return a list of station IDs from CSV file. 

    Args:
        station_id_filename: A CSV file name from which to read the station
            IDs from. Default is 'station_ids.csv'. 
        station_id_column_name: The column of the CSV file that contains
            the station IDs. Default is 'SITEID'.  

    Raises:
        FileNotFoundError: The specified CSV file was not found. 
        ValueError: The specified column is not in the CSV file. 
    """
    try:
        df = pd.read_csv(station_id_filename)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {station_id_filename}")
    
    if station_id_column_name in df.columns:
        station_list = df[station_id_column_name].tolist()
        station_ids = [x.lower() for x in station_list] # Convert all station IDs to lower case. 
        return station_ids
    else:
        raise ValueError(f"Specified station id column name '{station_id_column_name}' not present in dataset.")


def download_files(
    url: str, 
    download_dir: str, 
    is_silent: bool=True
) -> bool:
    """Download a file using curl into the specified directory.
    
    This function will download the d.gz file format from the NOAA-CORS 
    download url, unzip and convert the d file to an o file, and then 
    delete the zipped and d file versions, leaving the users with
    only o files for the date and station_id they requested. 

    Args:
        url: The full NOAA-CORS download url
        download_dir: Path to save the download file.
        is_silent: If True, supress curl output in terminal.
    
    Raises:
        SystemExit: If the download fails.
    """
    
    # Check if the file exists on the server.
    if not is_url_available(url):
        print(f'Skipping download after retries: {url}')
        return False
    
    try:
        os.makedirs(download_dir, exist_ok=True)
        file_name_gz = os.path.basename(url) # e.g., p1981000.25d.gz
        file_path_gz = os.path.join(download_dir, file_name_gz)
        file_path_d = file_path_gz[:-3] # Strip .gz
        file_name_d = os.path.basename(file_path_d)
        if is_silent:
            run_list = ["curl", "-s", "-o", file_name_gz, url]
        else: 
            run_list = ["curl", "-o", file_name_gz, url]

        subprocess.run(
            run_list, 
            cwd=download_dir, 
            check=True
        ) # Run the curl command. 

    except subprocess.CalledProcessError as e:
        print(f"File Download failed: {e}")
        return False
    
    # Unzip file and delete the zipped version.
    try:
        unzip_file(download_dir, file_name_gz)
        file_path_to_remove = os.path.join(download_dir, file_name_gz)
        is_file_removed = remove_file(file_path_to_remove)
    except Exception as e:
        print(f"Unzipping failed: {e}")
        return False

    # Convert 'd' file to 'o' and delete the 'd' file.
    try:
        obs_file_o = handle_hatanaka_rinex(file_name_d, download_dir)
        file_path_to_remove = os.path.join(download_dir, file_name_d)
        is_file_removed = remove_file(file_path_to_remove)
    except Exception as e:
        print(f"Conversion failed: {e}")
        return False
    
    return True # Return true if no issues occured. 
    

def main():
    start_download_date = pd.to_datetime("2020-4-10")
    end_download_date = pd.to_datetime("2020-4-30")
 
    dates_to_download_list = pd.date_range(
        start=start_download_date, 
        end=end_download_date
    )

    station_id_list = get_station_ids() 

    # Obtaining total expected download numbers for output. 
    total_expected_downloads = len(dates_to_download_list) * len(station_id_list)
    
    tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        for date in dates_to_download_list: # Looping throw each date and then through each station name to get data. 
            for station in station_id_list:
                url = get_combined_url(date, station)
                download_dir_name = f'daily/{date.year}/{date.dayofyear:03d}'
                tasks.append(executor.submit(download_files, url, download_dir_name)) # Appending to task array. This is used later in the code to check that all threads are finished before ending the program.

        num_successful_downloads = 0
        for task in as_completed(tasks): # Loop through completed threads and obtain output. 
            try: 
                if task.result():
                    num_successful_downloads += 1
            except Exception as e:
                print(f'Error with thread result: {e}')


    # Printing Metrics
    print(f"Downloading Completed!")
    print(f"Downloaded {num_successful_downloads} of {total_expected_downloads}")
    

if __name__ == "__main__":
    main()