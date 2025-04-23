import pandas as pd
import subprocess
import os
import sys
from concurrent.futures import ThreadPoolExecutor


def get_combined_url(
        date: pd.DatetimeIndex, 
        station_id: str
    ) -> str:

    download_year_full = date.year
    download_year_last_two = f"{date.year % 100:02d}"
    download_day = f"{date.dayofyear:03d}"

    # Command template: 
    # 'curl -O https://noaa-cors-pds.s3.amazonaws.com/rinex/2025/001/corv/corv0010.25d.gz'
    const_url = "https://noaa-cors-pds.s3.amazonaws.com/rinex"
    changing_url = f"{download_year_full}/{download_day}/{station_id}/{station_id}{download_day}0.{download_year_last_two}o.gz" # Use 'o' file type instead of 'd'. 
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
    ) -> None:
    """Download a file using curl into the specified directory.

    Args:
        url: The full NOAA-CORS download url
        download_dir: Path to save the download file.
        is_silent: If True, supress curl output in terminal.
    
    Raises:
        SystemExit: If the download fails.
    """
    try:
        os.makedirs(download_dir, exist_ok=True)
        file_name = os.path.basename(url)
        if is_silent:
            run_list = ["curl", "-s", "-o", file_name, url]
        else: 
            run_list = ["curl", "-o", file_name, url]
            
        subprocess.run(
            run_list, 
            cwd=download_dir, 
            check=True
        )
        print(f"Download Complete: {url}")
    except subprocess.CalledProcessError as e:
        print(f"File Download failed: {e}")
        sys.exit(1)
    

def main():
    start_download_date = pd.to_datetime("2025-4-10")
    end_download_date = pd.to_datetime("2025-4-30")
 
    dates_to_download_list = pd.date_range(
        start=start_download_date, 
        end=end_download_date
    )

    station_id_list = get_station_ids() 

    # Obtaining total expected download numbers for output. 
    total_expected_downloads = len(dates_to_download_list) * len(station_id_list)
    actual_download_iterator = 0
    tasks = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        for date in dates_to_download_list: # Looping throw each date and then through each station name to get data. 
            for station in station_id_list:
                url = get_combined_url(date, station)
                download_dir_name = f'daily/{date.year}/{date.dayofyear:03d}'
                tasks.append(executor.submit(download_files, url, download_dir_name)) # Appending to task array. This is used later in the code to check that all threads are finished before ending the program.
                actual_download_iterator += 1

        for task in tasks: # Ensuring the threads finish before the program ends. 
            task.result()

    # Printing Metrics
    print(f"Downloading Completed!")
    print(f"Downloaded {actual_download_iterator} of {total_expected_downloads}")
    

if __name__ == "__main__":
    main()