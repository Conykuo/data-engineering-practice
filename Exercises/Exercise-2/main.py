import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

def main():
    link = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    time_code = '2024-01-19 15:43' # Since datetime 2024-01-19 10:27 does not exist in the website anymore, I use a new random date to practice instead.

    response = requests.get(link)
    soup = BeautifulSoup(response.content, 'html.parser')
    desired_file = []
    for i in soup.find_all('tr'):
        tds = i.find_all('td',align='right')
        if tds and tds[0].text != '-':
            if tds[0].text == time_code:
                desired_file.append(link+i.find('a').text)

    def read_csv(file):
        return pd.read_csv(file,usecols=['HourlyDryBulbTemperature'])
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(read_csv, desired_file))
    df = pd.concat(dfs,ignore_index=True)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'],errors='coerce')
    print(f"Max HourlyDryBulbTemperature: {df['HourlyDryBulbTemperature'].max()}")


if __name__ == "__main__":
    main()
