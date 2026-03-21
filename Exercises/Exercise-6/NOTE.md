Notes
- zipfile.namelist(): prints all the file names in the zip file
- zipfile.open(x): choose which file(x) to open in the zip file.
- zipfile object:
  - zipfile.extract(): for writing to **disk only**, return file path (string).
  - zipfile.extractall(): extracting all files to **disk only**, return nothing.
  - zipfile.namelist(): return list of filenames inside zip.
  - zipfile.open(x): choose which file(x) to open in the zip file. stays **in memory**, return file-like object.
- splitlines(): output a list of string.
- rdd: spark object. A distributed list of strings across multiple 
  workers/machines, no structure yet. It acts like a stepping stone to convert it a dataframe. 
- coalesce vs. repartition
  - coalesce: make multiple pizza into 1, more suitable in this case because I need to go from many files into 1. (Lazy)
  - repartition: can make more or less pizza, useful when you need more, evenly sized pizza so machines can work in parallel effectively. (Active)
- window & row_number():
  - window defines the rules — "group by month, order by total_trips descending"
  - row_number().over(window) applies those rule to df1 and rank each row in the manner with withColumn('rank'...)