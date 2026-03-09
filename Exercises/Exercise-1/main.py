import requests, zipfile, os
from pathlib import Path

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # create a download folder if there isn't one
    folder_path = Path("downloads")
    if not folder_path.exists():
        folder_path.mkdir()

    for uri in download_uris:
        file_name = uri.split("/")[-1]

        if not Path(f"downloads/{file_name}").exists():
            response = requests.get(uri) # download the file to the folder

            with open(f"downloads/{file_name}","wb") as file: # zip files are binary data, so use "wb" to write them in binary mode
                file.write(response.content)

            # check if it's zip file
            if zipfile.is_zipfile(f"downloads/{file_name}"):
                # if it is zip file, proceed to unzip it
                with zipfile.ZipFile(f"downloads/{file_name}","r") as z:
                    z.extractall("downloads")
                os.remove(f"downloads/{file_name}") # delete the zip file
            else:
                os.remove(f"downloads/{file_name}")




if __name__ == "__main__":
    main()
