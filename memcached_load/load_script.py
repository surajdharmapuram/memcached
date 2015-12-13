import memcache
import sys
import random
import string
import uuid
from optparse import OptionParser

if len(sys.argv) < 3:
    print("Usage: <script> <load_file> <load_type> = {1 (for mass keys), 2 (for specific keys)}")
    sys.exit(1)

max_num_keys = 1000000
timeout = 500
mc = memcache.Client(["127.0.0.1:11211"])

def random_string(length):
    return ''.join(random.choice(string.ascii_uppercase) for i in range(length))

def mass_load():
    with open(sys.argv[1]) as f:
        for line in f:
            num_keys, num_bytes = map(int, line.split())
            for i in range(num_keys):
                key = str(random.randint(0, max_num_keys))
                value = random_string(num_bytes)
                mc.set(key, value, timeout)

def specific_load():
    with open(sys.argv[1]) as f:
        for line in f:
            key, value, timeout = line.split()
            mc.set(key, value, int(timeout))

if __name__ == "__main__":
    if (sys.argv[2] == "1"):
        mass_load()
    else:
        specific_load()
