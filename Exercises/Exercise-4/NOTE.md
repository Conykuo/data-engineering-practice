Lessons Learnt
- writerow vs. writerows:
    - writerow: treats the whole thing as one single row
    - writerows: Only accepts a list of lists. Each inner list is treated as one row.
      If you pass a plain string, it iterates over each character since strings are iterable.

- json.load vs. json.loads:
    - json.load: accepts a file object
    - json.loads: accepts a string

- isinstance: checks if a variable is of a certain type/class
  e.g. isinstance(value, dict) → True if value is a dict

- rglob: recursively searches through all files from the current folder and downwards
  e.g. Path('.').rglob('*.json') → finds all .json files in all subdirectories

- path.parent: the directory containing the file
  e.g. data/foo/bar.json → data/foo
- path.stem: the filename without the extension
  e.g. data/foo/bar.json → bar