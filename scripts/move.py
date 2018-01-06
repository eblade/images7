import os
import sys
import logging
import argparse
import json

# Logging
FORMAT = '%(asctime)s [%(threadName)s] %(filename)s +%(levelno)s ' + \
         '%(funcName)s %(levelname)s %(message)s'
logging.basicConfig(
    format=FORMAT,
    level=logging.DEBUG,
)

from images6.system import System
from images6.entry import Entry
from images6.date import Date
from images6.ingest import image

# Options
parser = argparse.ArgumentParser(usage='python -m images6')

parser.add_argument(
    '-c', '--config',
    default=os.getenv('IMAGES6_CONFIG', 'images.ini'),
    help='specify what config file to run on')

args = parser.parse_args()

# Config
system = System(args.config)
logging.info("*** Done setting up Database.")

logging.info("Loading entries...")
source_data_folder = '/home/johan/Pictures/images6/database/data'
ids = []
for r, ds, fs in os.walk(source_data_folder):
    for f in fs:
        if f.endswith('.json'):
            path = os.path.join(r, f)
            with open(path, 'rb') as f:
                s = f.read().decode('utf8')
                entry_data = json.loads(s)
                entry = Entry.FromDict(entry_data)
                entry.id = entry_data['id']
                print(entry.id)
                ids.append(int(entry.id))
                #system.entry.save(entry.to_dict())


print(max(ids))
exit()
logging.info("Loading dates...")
source_data_folder = '/home/johan/Pictures/images6/database/date'
for r, ds, fs in os.walk(source_data_folder):
    for f in fs:
        if f.endswith('.json'):
            path = os.path.join(r, f)
            with open(path, 'rb') as f:
                s = f.read().decode('utf8')
                date_data = json.loads(s)
                date = Date.FromDict(date_data)
                date_str = os.path.basename(path).split('.')[0]
                print(date_str)
                date.id = date_str
                system.date.save(date.to_dict())
