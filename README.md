# NOAA CORS RINEX Downloader

This script downloads RINEX observation files (`o`) from the NOAA CORS archive for a specified range of dates and a list of GNSS station IDs.

## Directory Structure

Downloaded files are saved in the following structure:

```
daily/
  └── 2025/
      └── 101/
          └── corv1010.25o
```

Where:
- `2025` is the year
- `101` is the day-of-year (DOY)
- `corv1010.25o` is the RINEX file for the `corv` station on that day

## Requirements

- Python 3
- crx2rnx executable (must be downloaded and placed in the project directory)
- pandas
- requests

Install dependencies:
```
pip install pandas
```

### Important
You will also need the CRX2RNX executable file. This can be downloaded here: `https://terras.gsi.go.jp/ja/crx2rnx.html`

Once downloaded, place the `CRX2RNX` file in your scripts working directory. This file will allow for the conversion from `d` to `o` file types. 


## How to Use

1. **Prepare a list of station IDs** in a CSV file (`station_ids.csv`) with a column named `SITEID`:
```
SITEID
corv
p181
abcd
```
Note, the CSV file and column names can be different but must be specified when calling the `get_station_ids` function. 

2. **Ensure that the `CRX2RNX` executable is in your Python scripts working directory.**
  
See 'Requirements' section for more details. 

3. **Edit the date range** inside the `main()` function:
```python
start_download_date = pd.to_datetime("2025-04-10")
end_download_date = pd.to_datetime("2025-04-30")
```

4. **Run the script**:
```
python3 download_noaa.py
```

5. The script will download all available `d.gz` RINEX files for each station and date combination, which will then be converted to `o` files. 

If a download is unavailable, a warning message will be outputted to the user. 



## Notes

- Only `o` RINEX observation files are downloaded. This means that the script provides unzipped files for direct use. 
- All station IDs are automatically converted to lowercase.

