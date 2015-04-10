KingDB
======

###What is KingDB?

**KingDB** is a fast on-disk persistent key-value store. You can embed it or use it as a library in your C++ applications.

**KingServer** is a server application that embeds KingDB and implements the Memcached protocol. It allows you to access your data through a network interface using whatever programming language you want. KingServer is not a distributed system: the data lives in a single machine.

###Why should I use KingDB?

- Fast for heavy write workloads and random reads.
- The architecture, code, and data format are simple.
- Multipart API to read and write large entries in smaller parts.
- Multiple threads can access the same database safely.
- Crash-proof: nothing ever gets overwritten.
- Iterators and read-only consistent snapshots.
- Compaction happens in a background thread, and does not block reads or writes.
- The data format allows hot backups to be made.
- Covered by unit tests.

###How fast is KingDB?

KingDB was benchmarked using the same test suite as LevelDB. On a Linux CentOS 6.5, for entries with 16-byte keys and 100-byte values (50 bytes after compression), the performance was:

| Workload            | Operations per second |
| ------------------: | :-------------------- |
|    Sequential reads |                  104k |
|        Random reads |                  203k |
|   Sequential writes |                  233k |
|       Random writes |                  251k |
|           Overwrite |                  250k |

For more details and a comparison with LevelDB, you can refer to the full [KingDB benchmarks](doc/bench/benchmarks.md).

###Where is the documentation?

You can learn more in the [KingDB documentation](doc/kingdb.md) and the [KingServer documentation](doc/kingserver.md).

###How do I install KingDB?

You can find intallation instructions in the [installation section](doc/kingdb.md#2-how-to-install-kingdb) of the documentation.

KingDB has no external dependencies and has been tested on:

- Mac OS X 10.9.5 with Apple LLVM version 6.0 (clang-600.0.51)
- Linux Ubuntu 14.04 x64 with GCC 4.9.2
- Linux CentOS 6.5 x86\_64 with GCC 4.9.2

If you are using GCC, update the Makefile and add \-fno\-builtin\-memcmp in the CFLAGS, and if you have tcmalloc on your system, add \-ltcmalloc to the LDFLAGS. This will give you a nice performance speed\-up.

###Where do I get help?

You can get help on the KingDB mailing list. (*coming soon*)
