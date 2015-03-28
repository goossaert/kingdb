KingDB
======

###What is KingDB?

**KingDB** is a fast on-disk persistent key-value store. You can embed it or use it as a library in your C++ applications.

**KingServer** is a server application that embeds KingDB and implements the Memcached protocol. It allows you to access your data through a network interface using whatever programming language you want. KingServer is not a distributed system: the data lives in a single machine.

###Why should I use KingDB?

- Fast for heavy write workloads and random reads.
- Multipart API to read and write large entries in smaller parts.
- Multiple threads can access the same database safely.
- Crash-proof: nothing ever gets overwritten.
- Iterators and read-only consistent snapshots.
- Background compaction.
- The data format allows hot backups to be made.
- Covered by unit tests.

###Where is the documentation?

You can find detailed the documentation here. (*coming soon*)

###How do I install KingDB?

Installation instructions are in the documentation, here. (*coming soon*)

KingDB has no external dependencies and has been tested on:

- Mac OS X 10.9.5 with Apple LLVM version 6.0 (clang-600.0.51)
- Linux Ubuntu 14.04 x64 with GCC 4.9.2

###Where do I get help?

You can get help on the KingDB mailing list. (*coming soon*)
