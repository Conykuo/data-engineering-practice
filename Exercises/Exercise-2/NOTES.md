## My Notes
- The problem statement requests timestamp `2024-01-19 10:27`, however this timestamp 
  no longer exists on the website as of 2026/03/13 (earliest available is `2024-01-19 14:47`). 
  I used `2024-01-19 15:43` as a substitute for practice purposes.
- The matching timestamp returned 262 CSV files to process. To speed up the reading process:
  - Used `ThreadPoolExecutor` to download and read files in parallel
  - Used `usecols` to load only the required `HourlyDryBulbTemperature` column
- Added `beautifulsoup4` to `requirements.txt` for HTML parsing and web scraping
