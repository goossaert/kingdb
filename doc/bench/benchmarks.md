Benchmarks of KingDB v0.9.0 and LevelDB v1.18 
=============================================

Date: April 9, 2015

## TL;DR

Benchmarks were run for different value sizes, from 100 bytes to 1MB, using the benchmark framework of LevelDB. The benchmark program for KingDB can be found in `doc/bench/db_bench_kingdb.cc`

- Random writes: KingDB is faster than LevelDB by 2-10x.
- Sequential writes: KingDB and LevelDB are equivalent.
- Overwrites: KingDB is faster than LevelDB by 2-14x
- Random reads: Before compaction, KingDB is faster than LevelDB by 1.5-3x. However after compaction, LevelDB is faster than KingDB by around 1.7x.
- Sequential reads: LevelDB is faster than KingDB by 3-50x. Yes, KingDB is really bad for sequential reads.
- Compaction: LevelDB has faster compactions than KingDB, by 3-4x.

## Description

The benchmarks were made over two systems, a Linux CentOS 6.4 and a Mac OS X 10.9.5.

### Linux
- Operating System: CentOS 6.4
- RAM: 128GB DDR3
- CPU: 12-core Intel Xeon E5-2640 @ 2.50GHz
- Compiler: GCC 4.9.2
- Compiler and linker options: \-O2 \-fno-builtin-memcmp \-ltcmalloc
- Storage: hard drive
- File system: ext3

### Mac
- Operating System: OS X 10.9.5
- RAM: 8GB DDR3
- CPU: 4-core Intel Core i7 @ 2.3GHz
- Compiler: Apple LLVM version 6.0
- Compiler and linker options: \-O2
- Storage: solid state drive
- File system: HFS+

### Key-value store options
- LevelDB v1.18 has a write cache of 4MB, and uses Snappy v1.1.2 for compression.
- KingDB v0.9.0 has a write cache of 4MB, uses LZ4 v1.3.0 for compression.


## Results

For each workload, multiple value sizes are tested. The performance of LevelDB and KingDB are compared using [fold change](http://en.wikipedia.org/wiki/Fold_change) metrics. The first fold change column refers to the Linux sytem, and the second fold change column refers to the Mac system. A positive fold change means that KingDB is faster than LevelDB, and a negative fold change means that LevelDB is faster than KingDB.

Except for the value size and the fold changes, the metric for all the values in table are in number of operations per second (ops/second).

|        Workload | Value size |  LevelDB Linux |   KingDB Linux | fold change |    LevelDB Mac |     KingDB Mac | fold change |
| --------------: | ---------: | -------------: | -------------: | ----------: | -------------: | -------------: | ----------: |
|         fillseq |       100b |         265957 |         233808 |      -1.14x |         224064 |         255885 |       1.14x |
|         fillseq |        1kb |          95721 |          99127 |       1.04x |         117827 |         151057 |       1.28x |
|         fillseq |      100kb |           1605 |           1827 |       1.14x |           1646 |           2953 |       1.79x |
|         fillseq |      256kb |            674 |            670 |      -1.01x |            690 |           1303 |       1.89x |
|         fillseq |      512kb |            336 |            368 |       1.10x |            317 |            649 |       2.05x |
|         fillseq |        1mb |            169 |            180 |       1.07x |            140 |            313 |       2.24x |
|      fillrandom |       100b |         127551 |         251635 |       1.97x |         199560 |         254647 |       1.28x |
|      fillrandom |        1kb |          13063 |         104493 |       8.00x |          19738 |         148214 |       7.51x |
|      fillrandom |      100kb |            299 |           1821 |       6.09x |            303 |           3265 |      10.78x |
|      fillrandom |      256kb |            167 |            743 |       4.45x |            269 |           1274 |       4.74x |
|      fillrandom |      512kb |             97 |            375 |       3.87x |             85 |            654 |       7.69x |
|      fillrandom |        1mb |             38 |            184 |       4.84x |             34 |            331 |       9.74x |
|       overwrite |       100b |         127194 |         250689 |       1.97x |         176056 |         254971 |       1.45x |
|       overwrite |        1kb |          13582 |         108096 |       7.96x |          15211 |         142877 |       9.39x |
|       overwrite |      100kb |            229 |           1806 |       7.89x |            242 |           3462 |      14.31x |
|       overwrite |      256kb |            117 |            738 |       6.31x |            149 |           1308 |       8.78x |
|       overwrite |      512kb |             71 |            374 |       5.27x |             64 |            652 |      10.19x |
|       overwrite |        1mb |             23 |            186 |       8.09x |             25 |            334 |      13.36x |
|      readrandom |       100b |         147907 |         203417 |       1.38x |         130667 |         178890 |       1.37x |
|      readrandom |        1kb |          79057 |         130582 |       1.65x |          72228 |          91810 |       1.27x |
|      readrandom |      100kb |           4211 |           9480 |       2.25x |           4700 |          11843 |       2.52x |
|      readrandom |      256kb |           2812 |           4054 |       1.44x |           4480 |           5399 |       1.21x |
|      readrandom |      512kb |            737 |           2122 |       2.88x |            925 |           2567 |       2.78x |
|      readrandom |        1mb |            636 |            954 |       1.50x |            628 |           1237 |       1.97x |
|         readseq |       100b |        3289473 |         104482 |     -31.48x |        3649635 |          76952 |     -47.43x |
|         readseq |        1kb |         660501 |          83187 |      -7.94x |         930232 |          55509 |     -16.76x |
|         readseq |      100kb |          12761 |           5485 |      -2.33x |          20774 |           8499 |      -2.44x |
|         readseq |      256kb |           6208 |           2116 |      -2.93x |           7550 |           3767 |      -2.00x |
|         readseq |      512kb |           4145 |           1308 |      -3.17x |           4862 |           1939 |      -2.51x |
|         readseq |        1mb |           2047 |            531 |      -3.85x |           1750 |            930 |      -1.88x |


The benchmarks for the random and sequential reads were re-run after a compaction process:

|        Workload | Value size |  LevelDB Linux |   KingDB Linux | fold change |    LevelDB Mac |     KingDB Mac | fold change |
| --------------: | ---------: | -------------: | -------------: | ----------: | -------------: | -------------: | ----------: |
|      readrandom |       100b |         202306 |         219925 |       1.09x |         195083 |         183049 |      -1.07x |
|      readrandom |        1kb |         192752 |         152858 |      -1.26x |         173190 |         148875 |      -1.16x |
|      readrandom |      100kb |          13863 |           8679 |      -1.60x |          19809 |          12987 |      -1.53x |
|      readrandom |      256kb |           4285 |           2422 |      -1.77x |           8890 |           5462 |      -1.63x |
|      readrandom |      512kb |           2784 |           1704 |      -1.63x |           4623 |           2788 |      -1.66x |
|      readrandom |        1mb |           1461 |            843 |      -1.73x |           2402 |           1390 |      -1.73x |
|         readseq |       100b |        3717472 |         489715 |      -7.59x |        4366812 |         239923 |     -18.20x |
|         readseq |        1kb |        1242236 |         320102 |      -3.88x |        1287001 |         188714 |      -6.82x |
|         readseq |      100kb |          26271 |           7537 |      -3.49x |          32398 |           9355 |      -3.46x |
|         readseq |      256kb |          11467 |           2050 |      -5.59x |          13220 |           3799 |      -3.48x |
|         readseq |      512kb |           2871 |           1191 |      -2.41x |           7270 |           2006 |      -3.62x |
|         readseq |        1mb |           3021 |            683 |      -4.42x |           3274 |            973 |      -3.36x |

