import argparse
import random
import MySQLdb
from time import time

parser = argparse.ArgumentParser(description='Benchmark mySQL')
parser.add_argument('--keys', type=int, 
                    help='Number of keys to put in to mySQL')
parser.add_argument('--host',
                    help='Host to connect to')
parser.add_argument('--gets', type=int,
                    help='Number of gets to perform')
parser.add_argument('--focus', type=float,
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

# Connect to mySQL 
connection = MySQLdb.connect(host=host, user="testuser", passwd="testpass", db="test")
cursor = connection.cursor()

# Add all the keys if desired
if do_insert:
    cursor.execute("TRUNCATE TABLE keyvalue")
    batch_size = 100
    i = 0
    start = time()
    sql = "INSERT INTO keyvalue(`key`, value) VALUES %s"
    sql %= ",".join(["(%s, %s)"] * batch_size)
    while i < keys:
        to_insert = []
        for j in range(batch_size):
            to_insert += [i, 'Mary had a little lamb. '*100]
            i += 1
        cursor.execute(sql, to_insert)
        i += batch_size
    end = time()
    print '%d documents inserted took %.2fs' % (keys, end - start)

# Now get the keys in a random order
sql = "SELECT value FROM keyvalue WHERE `key`=%s"
start = time()
for i in range(gets):
    focussed_get = random.randint(0, 100) != 0
    if focussed_get:
        key = random.randint(0, keys * focus / 100.0) 
    else:
        key = random.randint(0, keys)
    cursor.execute(sql, [key])
    cursor.fetchall()
end = time()
print '%d gets (focussed on bottom %d%%) took %.2fs' % (gets, focus, end - start)
cursor.close()
