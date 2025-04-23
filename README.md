# NOAA CORS RINEX Downloader

This script downloads RINEX observation files (`.o.gz`) from the NOAA CORS archive for a specified range of dates and a list of GNSS station IDs.

## Directory Structure

Downloaded files are saved in the following structure:

```
daily/
  └── 2025/
      └── 101/
          └── corv1010.25o.gz
```

Where:
- `2025` is the year
- `101` is the day-of-year (DOY)
- `corv1010.25o.gz` is the RINEX file for the `corv` station on that day

## Requirements

- Python 3
- `pandas`

Install dependencies:
```
pip install pandas
```


## How to Use

1. **Prepare a list of station IDs** in a CSV file (`station_ids.csv`) with a column named `SITEID`:
```
SITEID
corv
p181
abcd
```
Note, the CSV file and column names can be different but must be specified when calling the `get_station_ids` function. 

2. **Edit the date range** inside the `main()` function:
```python
start_download_date = pd.to_datetime("2025-04-10")
end_download_date = pd.to_datetime("2025-04-30")
```

3. **Run the script**:
```
python3 download_noaa.py
```

4. The script will download all available `.o.gz` RINEX files for each station and date combination.



## Notes

- Only `.o.gz` RINEX observation files are downloaded.
- Files are currently **not** skipped if the server returns a missing file error.
- All station IDs are automatically converted to lowercase.

