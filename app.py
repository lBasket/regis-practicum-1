import argparse
import json
from datetime import datetime
import requests
import socket
import os
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import time
from datetime import datetime as dt
from datetime import timedelta

def parse_args():
    parser = argparse.ArgumentParser(
        prog='Scraper',
        description='parsin args to scrape data'
    )

    parser.add_argument(
        '-r', '--report',
        dest='report',
        required=True,
        help='report name AKA dict key from JSON config'
    )
    parser.add_argument(
        '-sd', '--startdate',
        dest='startdate',
        required=True,
        type=lambda x: datetime.strptime(x, '%Y%m%d%H%M'),
        help='startdate of data to scrape, format YYYYMMDD HHMI'
    )
    parser.add_argument(
        '-ed', '--enddate',
        dest='enddate',
        required=True,
        type=lambda x: datetime.strptime(x, '%Y%m%d%H%M'),
        help='enddate of data to scrape, format YYYYMMDD HHMI'
    )
    parser.add_argument(
        '-e', '--entity',
        dest='entity',
        help='zero or more entities to use in API calls'
    )
    parser.add_argument(
        '-c', '--config',
        dest='config',
        default='config.json',
        help='filepath of config json to use.'
    )

    args = parser.parse_args()

    return args

def read_json(filename):
    '''
    Reads a json and converts into a dict
    '''
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    return data

def update_url(url, startdate, enddate, entities):
    '''
    Replaces predetermined strings with dates and entities for API calls
    '''
    replaces = [
        ['%sY', startdate.strftime('%Y')],
        ['%sm', startdate.strftime('%m')], # month
        ['%sD', startdate.strftime('%d')],
        ['%sH', startdate.strftime('%H')],
        ['%sM', startdate.strftime('%M')], # minutes
        ['%eY', enddate.strftime('%Y')],
        ['%em', enddate.strftime('%m')], # month
        ['%eD', enddate.strftime('%d')],
        ['%eH', enddate.strftime('%H')],
        ['%eM', enddate.strftime('%M')], # minutes
        ['%entity', entities]
    ]

    for replace_rec in replaces:
        if all(replace_rec):
            url = url.replace(replace_rec[0], replace_rec[1])

    return url

def scrape_data(rpt):
    #rudimentary token passing for now
    hdr=None
    if 'weather' in rpt['report']:
        hdr = {'token': 'iiKEZnxwJlANCKnCZAGyjHBSBMuUYOlM'}

    rpt['url'] = update_url(rpt['url'], rpt['startdate'], rpt['enddate'], rpt['entity'])
    print(f'Making API call at {rpt["url"]}')

    req = requests.get(rpt['url'], headers=hdr)

    print(f'State code from response {req.status_code}')
    if req.status_code == 200:
        return req.content

    raise ValueError(req.content)

def get_path(report_name):
    host = socket.gethostname()

    paths = {
        'homelabr710': '/home/basket/regis/scrapes/data/'
    }
    path = os.path.join(paths[host], report_name)

    return path


def save_data(rpt, data):
    # unzip
    # https://stackoverflow.com/questions/5710867/downloading-and-unzipping-a-zip-file-without-writing-to-disk
    
    save_path = os.path.join('data/', rpt['report']) 
    os.makedirs(save_path, exist_ok=True)
    
    if 'weather' not in rpt['report']:
        print(f'Writing data to {save_path}')

        with ZipFile(BytesIO(data)) as zip_info:
            zip_info.extractall(path=save_path)
    else:
        now = datetime.now().strftime('%Y%m%d_%H%M%s')
        filename = rpt['report'] + '_data_' + now + '.csv'

        save_path = os.path.join(save_path, filename)

        data = json.loads(data.decode('utf-8'))

        df = pd.DataFrame(data['hourly'])

        keys = [x for x in data.keys() if 'hourly' not in x]

        for k in keys:
            df[k] = data[k]

        print(f'Writing data to {save_path}')

        df.to_csv(save_path, index=False)


if __name__ == '__main__':
    js = read_json('config.json')

    #configs = ['RT_pricing', 'DA_pricing']
    #nodes = ['DVLCYN_1_UNITS-APND', 'DIABLO1_7_B1', 'SLSTR1_2_SOLAR1-APND']
    configs = ['Actual_load', 'Fcast_load']
    nodes = ['TAC_PGE', 'TAC_LADWP']


    for config in configs:
        rpt = js[config]
        rpt['report'] = config
        base_url = rpt['url']
        
        for node in nodes:
            start = dt.strptime('01012020', '%m%d%Y')
            
            while start <= dt.strptime('01012023', '%m%d%Y'):
                rpt['startdate'] = start
                start = start + timedelta(days=14)

                rpt['enddate'] = start
                rpt['entity'] = node
                rpt['url'] = base_url

                data = scrape_data(rpt)

                save_data(rpt, data)

                time.sleep(8)

                            

    configs = ['Actual_weather', 'Forecast_weather']
    nodes = ['35.2108,-120.8561', '34.2056,-117.334', '34.83,-118.399']


    for config in configs:
        rpt = js[config]
        rpt['report'] = config
        base_url = rpt['url']
        
        for node in nodes:
            start = dt.strptime('01012020', '%m%d%Y')
            
            while start <= dt.strptime('01012023', '%m%d%Y'):
                rpt['startdate'] = start
                start = start + timedelta(days=14)

                rpt['enddate'] = start
                rpt['entity'] = node
                rpt['url'] = base_url

                data = scrape_data(rpt)

                save_data(rpt, data)

                time.sleep(8)

