#!/usr/bin/python

# This program reads outputs from the LevelDB benchmarks and generates
# a table in Markdown format to compare the performance of LevelDB
# and KingDB.

# In the case of the comparison between LevelDB and KingDB,
# it makes more sense to use a fold change rather than
# a percentage change:
#
# http://en.wikipedia.org/wiki/Fold_change
# http://en.wikipedia.org/wiki/Relative_change_and_difference

import sys

def fold_change(ldb, kdb):
  growth = 0
  ldb = float(ldb)
  kdb = float(kdb)
  if (ldb > kdb):
    growth = - ldb / kdb
  else:
    growth = kdb / ldb
  return growth

def perc_change(ldb, kdb):
  ldb = float(ldb)
  kdb = float(kdb)
  return (kdb - ldb) / ldb * 100.0


f = open(sys.argv[1], "r")
lines_mac = f.readlines()
f.close()

f = open(sys.argv[2], "r")
lines_linux = f.readlines()
f.close()

lines = lines_mac + lines_linux

perf = {}

database = None
workload = None
cpu = None
size = None
has_compaction_passed = False

for line in lines:
  line = line.strip()
  items = line.split()
  if line.startswith('LevelDB'):
    database = 'leveldb'
    has_compaction_passed = False
  elif line.startswith('KingDB'):
    database = 'kingdb'
    has_compaction_passed = False

  if line.startswith('CPU:'):
    cpu = line.split(' ', 1)[1].strip()
    if cpu.startswith('24 *'):
      cpu = 'linux'
    elif cpu.startswith('mac_nodatacopy'):
      cpu = 'mac_nodatacopy'
    elif cpu.startswith('mac_ndc_snappy'):
      cpu = 'mac_ndc_snappy'
    elif cpu.startswith('mac'):
      cpu = 'mac'

  if line.startswith('Values'):
    size = items[1]

  if len(items) > 3 and items[3] == 'micros/op;':
    workload = items[0]
    ops = 1.0 / (float(items[2])/1000000.0)
    ops = int(ops)
    if workload == 'fillrandsync':
      workload = 'fillsync'
    if has_compaction_passed:
      workload += '-ac'
    if workload == "compact":
      has_compaction_passed = True
    if workload not in perf:
      perf[workload] = {}
    if size not in perf[workload]:
      perf[workload][size] = {}
    if database not in perf[workload][size]:
      perf[workload][size][database] = {}
    if cpu not in perf[workload][size][database]:
      perf[workload][size][database][cpu] = {}
      perf[workload][size][database][cpu] = ops


sizes = {100: '100b',
         1024: '1kb',
         1024*100: '100kb',
         1024*256: '256kb',
         1024*512: '512kb',
         1024*1024: '1mb'
        }


line =  "| %15s | %10s | %14s | %14s | %11s | %14s | %14s | %11s |" % ('Workload', 'Value size', 'LevelDB Linux', 'KingDB Linux', 'fold change', 'LevelDB Mac', 'KingDB Mac', 'fold change')
print line
line_separator = "| %015d | %010d | %014d | %014d | %011d | %014d | %014d | %011d |" % (0, 0, 0, 0, 0, 0, 0, 0)
line_separator = line_separator.replace('0', '-')
print line_separator

for workload in ['fillseq', 'fillrandom', 'overwrite', 'readrandom', 'readseq', 'readrandom-ac', 'readseq-ac']:
  if any( substring in workload for substring in ['100K', 'snappy', 'crc32','acquire', 'reverse', 'fillseqsync', 'compact'] ):
    continue
  for size in [100, 1024, 1024*100, 1024*256, 1024*512, 1024*1024]:
    size_human = sizes[size]
    size = str(size)
    ldb_linux = -1
    ldb_mac = -1
    kdb_linux = -1
    kdb_mac = -1
    try:
      ldb_linux = perf[workload][size]['leveldb']['linux']
    except:
      pass
    try:
      ldb_mac = perf[workload][size]['leveldb']['mac']
    except:
      pass
    try:
      kdb_linux = perf[workload][size]['kingdb']['linux']
    except:
      pass
    try:
      kdb_mac = perf[workload][size]['kingdb']['mac']
    except:
      pass
    
    perc_linux = fold_change(ldb_linux, kdb_linux)
    perc_mac = fold_change(ldb_mac, kdb_mac)
    line =  "| %15s | %10s | %14s | %14s | % 10.2fx | %14s | %14s | % 10.2fx |" % (workload, size_human, ldb_linux, kdb_linux, perc_linux, ldb_mac, kdb_mac, perc_mac)
    print line
    
