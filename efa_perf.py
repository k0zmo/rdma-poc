#!/usr/bin/python3
from optparse import OptionParser
from subprocess import Popen, PIPE
import sys
import time
import re
import math
from collections import defaultdict
import tempfile

def thous(x, sep=',', dot='.'):
    frac, num = math.modf(x)
    num = str(int(num))
    frac = int(frac * 100)
    num = re.sub(r'(\d{3})(?=\d)', r'\1'+sep, num[::-1])[::-1]
    if frac > 0:
        num += dot + str(frac)
    return num

def get_stats(intf, keys = None):
    with tempfile.NamedTemporaryFile(delete=True) as stdout_pipe:
        process = Popen('rdma -p statistic show link %s' % intf,
                shell=True, bufsize=0,
                stdout=stdout_pipe)
        rc = process.wait()
        stdout_pipe.seek(0)
        if (rc):
            print(("error running rdma(%d):" % (process.returncode)))
            sys.exit(0)

        map = {}
        output = stdout_pipe.readlines()[1:]
    for line in output:
        key, val = line.strip().decode('utf8').split(" ")

        if not keys == None:
            keys += [key]

        if val == "":
            val = "0"

        map[key] = int(val)
    return map

parser = OptionParser(usage="%prog -i <interface> [options]", version="%prog 1.0")
parser.add_option("-i", "--interface", dest="intf", help="Interface name")
parser.add_option("-t", "--interval", dest="interval", default=1,
        help="Interval between measurements in seconds")
parser.add_option("-c", "--count", dest="count", default=-1, type="int",
        help="Exit counter - exit after counting number of intervals ( default is -1: do not exit) ")

(options, args) = parser.parse_args()

if (options.intf == None):
    print("Interface name is required")
    parser.print_usage()
    sys.exit(1)

print("Initializing efa_perf...")

# keys must be ordered, so can't use 'for key in map'
keys = []
prev = get_stats(options.intf, keys)

count = int(options.count)

if count < -1 or count == 0:
    print("Error, please use positive value for \"count\" or \"-1\" for no exit ")
    sys.exit(1)
    
print("Sampling started.")

while count != 0:
    time.sleep(float(options.interval))
    count -= 1

    curr = get_stats(options.intf)
    secs = float(options.interval)

    up_bw = defaultdict(int)
    up_packets = defaultdict(int)
    total_bw = 0
    total_packets = 0
    something_printed = False
    for key in keys:

        bw = (curr[key]-prev[key]) / secs
        if (bw > 0):
            if "bytes" in key:
                # Calculate throughput rate in Mbps from the bytes counter
                print(("%30s: %-20s = %-20s" % (key, thous(bw) + " Bps",
                    thous(bw * 8 / 1000000) + " Mbps")))
            else:
                print(("%30s: %s" % (key, thous(bw))))
                
            something_printed = True

    prev = curr
    if something_printed:

        print("--------")
        sys.stdout.flush()
