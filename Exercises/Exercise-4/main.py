import json, glob, csv, os
from pathlib import Path

def main():

    def flatten_file(path):
        with open(path) as f:
            file = json.load(f)
        new_data = dict()
        for key, value in file.items():
            if not isinstance(value,dict):
                new_data[key] = value
            else:
                for k2,v2 in value.items():
                    new_data[f"{key}_{k2}"] = v2
        if 'year' in new_data:
            new_data['year'] = new_data['year'][:4]
        return new_data

    def generate_csv(path,flatten_data):
        with open(f"{path.parent}/{path.stem}.csv",'w',newline='') as csvfile:
            cols_to_drop = ['geolocation_type','geolocation_coordinates']
            headers = [k for k,v in flatten_data.items() if k not in cols_to_drop]
            value = [v for k,v in flatten_data.items() if k not in cols_to_drop]
            writer = csv.writer(csvfile)
            writer.writerow(headers)
            writer.writerow(value)

    for path in Path('.').rglob('*.json'):
        print(path)
        flatten_dict = flatten_file(path)
        print(flatten_dict)
        generate_csv(path,flatten_dict)


if __name__ == "__main__":
    main()
