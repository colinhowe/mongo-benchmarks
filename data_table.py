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
parser.add_argument('--do_insert', const=True, default=False,
                    action='store_const',
                    help='Do inserts')
parser.add_argument('--data_table', const=True, default=False,
                    action='store_const',
                    help='Use a data table for text')

args = parser.parse_args()

host = args.host
keys = args.keys
gets = args.gets
do_insert = args.do_insert
data_table = args.data_table

chunks = 100

# Connect to mongo
connection = Connection(host)
db = connection.bench

# Add all the keys if desired
if do_insert:
    db.drop_collection('docs')
    db.drop_collection('data')
    docs = db.docs
    docs.create_index([('key', ASCENDING)])
    batch_size = 1000
    i = 0
    last_log = 0
    start = time()

    if data_table:
        data = db.data
        while i < keys:
            if i - last_log > 10000:
                last_log = i
                print '%d/%d inserted' % (i, keys)
            data_objs = []
            for j in range(batch_size):
                data_objs.append({
                    'text': '0123456789' * 100,
                })
            data_ids = data.insert(data_objs, safe=True)
            
            to_insert = []
            for data_id in data_ids:
                to_insert.append({
                    'key': i,
                    'chunk': i % chunks,  # Break data into 100 chunks
                    'data_obj': data_id,
                })
                i += 1
            docs.insert(to_insert, safe=True)
    else:
        while i < keys:
            if i - last_log > 10000:
                last_log = i
                print '%d/%d inserted' % (i, keys)
            to_insert = []
            for j in range(batch_size):
                to_insert.append({
                    'key': i,
                    'chunk': i % chunks,  # Break data into 100 chunks
                    'text': '0123456789' * 100
                })
                i += 1
            docs.insert(to_insert, safe=True)
    end = time()
    print '%d documents inserted took %.2fs' % (keys, end - start)
else:
    docs = db.docs

if data_table:
    data = db.data

# Perform fetches of 10 documents at a time from the database. The 'chunk'
# for each fetch is random and the results is ordered by key. This ensures
# that an index is used for some of the data but not all

start = time()
c = 0
for i in range(gets):
    if i % 1000 == 0:
        print '%d/%d gets' % (i, gets)
    start_key = random.randint(0, keys)
    chunk = random.randint(0, chunks)
    if data_table:
        results = docs.find({'key': { '$gt': start_key}, 'chunk': chunk}).limit(10)
        ids = [result['data_obj'] for result in results]
        texts = data.find({'_id': { '$in': ids }})
        for text in texts:
            c += len(text['text'])
    else:
        results = docs.find({'key': { '$gt': start_key}, 'chunk': chunk}).limit(10)
        for result in results:
            c += len(result['text'])

end = time()
print 'Check value: %d' % c
print '%d gets took %.2fs' % (gets, end - start)
