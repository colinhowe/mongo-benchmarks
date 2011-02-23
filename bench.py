import random
import argparse
from time import time

from pymongo import Connection, ASCENDING

parser = argparse.ArgumentParser(description='Benchmark mongo')
parser.add_argument('--keys', type=int, 
                    help='Number of keys to put in to Mongo')
parser.add_argument('--host',
                    help='Host to connect to')
parser.add_argument('--gets', type=int,
                    help='Number of gets to perform')
parser.add_argument('--focus', type=int,
                    help='Percentage to focus reads on')
parser.add_argument('--do_insert', const=True, default=False,
                    action='store_const',
                    help='Do inserts')
args = parser.parse_args()

host = args.host
keys = args.keys
gets = args.gets
focus = args.focus
do_insert = args.do_insert

# Connect to mongo
connection = Connection(host)
db = connection.bench

# Add all the keys if desired
if do_insert:
    db.drop_collection('docs')
    docs = db.docs
    docs.create_index([('key', ASCENDING)])
    batch_size = 100
    i = 0
    start = time()
    while i < keys:
        to_insert = []
        for j in range(batch_size):
            to_insert.append({
                'key': i,
                'text': 'Mary had a little lamb. '*100
            })
            i += 1
        docs.insert(to_insert)
        i += batch_size
    end = time()
    print '%d documents inserted took %.2fs' % (keys, end - start)
else:
    docs = db.docs

# Now get the keys in a random order
start = time()
for i in range(gets):
    focussed_get = random.randint(0, 100) != 0
    if focussed_get:
        key = random.randint(0, keys * focus / 100.0) 
    else:
        key = random.randint(0, keys)
    docs.find_one({'key': key})
end = time()
print '%d gets (focussed on bottom %d%%) took %.2fs' % (gets, focus, end - start)
