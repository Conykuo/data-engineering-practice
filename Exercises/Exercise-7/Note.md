Note
- Difference between with open vs. with zip_object.open()
  - with open(): looking for file on disk
  - with zipfile_object.open(): the open() method on zipfile_object direct read the file inside the zipfile in memory.