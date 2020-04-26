import concurrent
import os
import csv
import shutil
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock
from time import sleep
import requests
import pycountry
import geopandas
import numpy as np
import json

urls = [
 # LIST OF CLOUD FUNCTIONS URLS
    'https://URL/pytrends1',
]


def get_all_pairs(path='data.csv'):
    if not os.path.exists(path):
        return set()

    pairs = set()
    input_file = csv.DictReader(open(path))
    for line in input_file:
        pairs.add((line['region'], line['kw']))
    return pairs


lock = Lock()


def to_csv(data, symptom_name, path='data.csv'):
    lock.acquire()
    try:
        keys = ['date', 'kw', 'region', 'value']
        for d in data:
            d['kw'] = symptom_name
        if not os.path.exists(path):
            with open(path, 'w', newline='') as file:
                writer = csv.DictWriter(file, keys)
                writer.writeheader()

        with open(path, 'a', newline='') as file:
            writer = csv.DictWriter(file, keys)
            writer.writerows(data)
    finally:
        lock.release()


def fetch_data(country, symptom, symptoms_name):
    while True:
        url = np.random.choice(urls)
        print(f'fetching data for ({country}, {symptom}|{symptoms_name}) from {url}')
        res = requests.post(url,
                            json={
                                'kw': symptom,
                                'region': country,
                                'fromDate': '2004-1-1'
                            })
        try:
            data = res.json()
            if len(data) == 0:
                print(f'Complete for ({country}, {symptom}|{symptoms_name}): no records received')
                return data
            to_csv(data, symptoms_name)

            # debugging printing
            first = data[0]
            values = np.array([x['value'] for x in data])
            avg = np.average(values)
            mean = np.median(values)
            nonzero = np.count_nonzero(values)

            print(f'Complete for ({country}, {symptoms_name}|{symptom}): '
                  f'len={len(values)}, first={first}, avg={avg}, mean={mean}, nonzero={nonzero}')
            return data
        except Exception as e:
            print(f'Complete for ({country}, {symptom}|{symptoms_name}) result is: {res.text}')
            if '429' not in str(res.content):
                print(f'not 429, skipping {e}')
                return
            print(e)
            sleep(60)  # Maybe make every sleep longer than the previous?


def filter_data(data_path, backup_path, threshold):
    assert os.path.exists(data_path), f'Could not find file {data_path}'

    print(f'start filtering file: {data_path}, backup:{backup_path}, threshold:{threshold}')


    pair_count = dict()
    print(f'backup {data_path} to {backup_path}')
    dict_file = csv.DictReader(open(data_path))
    for line in dict_file:
        pair = (line['region'], line['kw'])
        pair_count[pair] = pair_count.get(pair, 0) + 1

    # Filter pairs according to to threshold
    pair_count = filter(lambda x: x[1] > threshold, pair_count.items())
    pair_count = map(lambda x: x[0], pair_count)
    pair_count = set(pair_count)

    # Filter file rows
    dict_file = csv.DictReader(open(backup_path))
    data = filter(lambda line: (line['region'], line['kw']) in pair_count, dict_file)
    data = list(data)

    # Todo check if need to create a backup file (if not log and skip)
    # Create a backup file just in case
    assert not os.path.exists(backup_path), f'could not back to {backup_path}, file already exist'
    shutil.copyfile(data_path, backup_path)

    # Rewrite content to file
    keys = ['date', 'kw', 'region', 'value']
    with open(data_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(data)


if __name__ == '__main__':
    file_path = 'data.csv'
    # pairs = get_all_pairs(file_path)
    # print(f'len before filtering :{len(pairs)}')
    # filter_data(file_path, 'back_up_data.csv', 4000)
    # pairs = get_all_pairs(file_path)
    # print(f'len after filtering :{len(pairs)}')

    with open('symptoms.json', 'r') as file:
        symptoms = json.load(file)

    for symptom in symptoms:
        print(symptom['name'], symptom['code'])

    countries = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    countries = countries.sort_values(by=['pop_est'], ascending=False)
    countries = list(countries['iso_a3'])
    countries = filter(lambda x: x in [c.alpha_3 for c in pycountry.countries], countries)
    countries = map(lambda x: [c.alpha_2 for c in pycountry.countries if c.alpha_3 == x][0], countries)
    countries = [''] + list(countries)
    print(len(countries), 'countries:', countries)
    print(len(symptoms), 'symptoms:', symptoms)

    pairs = get_all_pairs(file_path)
    print(len(pairs), pairs)
    sleep(30)
    # countries = [""]
    # symptoms = [{'name': 'Sore throat', 'code': '/m/0b76bty'}]
    with ThreadPoolExecutor(max_workers=300) as executor:
        for country in countries:
            for symptom in symptoms:
                if (country, symptom['name']) not in pairs:
                    print(f'{(country, symptom)}')
                    executor.submit(fetch_data, country, symptom['code'], symptom['name'], )
                    # threads.append(Thread(target=fetch_data, args=(country, symptom,)))
                else:
                    print(f'Skipping {(country, symptom["name"])}')
