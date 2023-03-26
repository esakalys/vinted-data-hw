
import os
import csv

# Reads a dataset and return a list of key value pairs
def read_ds(input):
    output = []
    with open(input) as file:
        ds = csv.DictReader(file)
        for row in ds:
            output.append(row)
    return output

# Flattens a list with sublists in it
def flatten(list):
    return [item for sublist in list for item in sublist]

# Writes a list of dictionaries to the output csv file
def write_ds(ds, output):
    filename = f'{output}/dataset.csv'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as file:
        columns = ds[0].keys()
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for entry in ds:
            writer.writerow(entry)
