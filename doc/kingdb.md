Documentation of KingDB v0.9.0
==============================

**IMPORTANT:** This document is still a work in progress.

##Table of Contents

**[1. Why should I use KingDB?](#1-why-should-i-use-kingdb)**  
**[2. How to install KingDB?](#2-how-to-install-kingdb)**  
**[3. How do I compile my programs with KingDB?](#3-how-do-i-compile-my-programs-with-kingdb)**  
**[4. Basic API usage](#4-basic-api-usage)**  
**[5. The ByteArray class](#5-the-bytearray-class)**  
**[6. Multipart API](#6-multipart-api)**  
**[7. Logging with Syslog](#7-logging-with-syslog)**  
**[8. The KingServer network server](#8-the-kingserver-network-server)**  
**[9. Options](#9-options)**  
**[10. Diving into the source code](#10-diving-into-the-source-code)**  


##1. Why should I use KingDB?

###KingDB is simple
The architecture, code, and data format of KingDB are simple. You do not need to be a system programming expert or a storage engineer to understand how it works and tune it to your needs.

###Fast for writes and random reads
Under the hood, KingDB uses log-structured storage, which makes writes very fast. An in-memory hash table indexes entries making random reads very fast.

###Multipart API
KingDB has a multipart API for both the reads and writes: you can access your data in small parts, without having to store all the data at once in memory, making it easy to work with large entries without killing the caches of your CPU or make your program timeout.

### Snapshots and Iterators
KingDB has read-only snapshots, that offer consistent views of a database. KingDB also has iterators so you can iterate over all your entries. Under the hood, KingDB is just a hash table and memory mapped files, which means you cannot access your data ordered by key. If you need a database that can give fast sequential reads ordered by keys, you should checkout another key-value store called LevelDB. 

### Background compaction
Regularly, KingDB runs a compaction process in background to recover unused disk space and make sure the database is stored in the most compact way possible. Please note, compaction is not compression.

###Covered with unit tests
The latest version of KingDB is 0.9.0, which is a beta version. All changes on KingDB are tested using unit tests.

###KingDB is single-machine only
KingDB is **not** a distributed and replicated multi-node datastore such as Cassandra or Riak. KingDB was designed to be a storage engine that lives on a single machine. If the disk on that machine dies, you lose your data. Luckily, the data format is loose enough that it is easy to do incremental backups and keep a copy of your data on a secondary machine if you need to.


##2. How to install KingDB

KingDB has no external dependencies and has been tested on:

- Mac OS X 10.9.5 with Apple LLVM version 6.0 (clang-600.0.51)
- Linux Ubuntu 14.04 x64 with GCC 4.9.2

Because KingDB uses C++11, you need GCC version 4.9.2 or greater. The following commands will compile KingDB as a static library, and will install it on your computer. This will also install the `kingserver` program.

    $ tar zxvf kingdb.tar.gz
    $ cd kingdb
    $ make
    $ sudo make install

##3. How do I compile my programs with KingDB?

Once you have compiled KingDB and installed the static library by following the instructions above, you can compile your own programs by linking them with the KingDB library.

With LLVM:

    $ g++ -std=c++11 -I/usr/local/include/kingdb -lkingdb kingdb_user.cc -o kingdb_user

With GCC:

    $ g++ -std=c++11 -I/usr/local/include/kingdb kingdb_user.cc -o kingdb_user -Wl,--no-as-needed -L/usr/local/lib -lpthread -lkingdb 

For an example of what a user program would look like, you can refer to the `kingdb_user.cc` file in the unit-tests directory.



##4. Basic API usage

###No pointers, everything is an object

You don't need to worry about memory management. KingDB will never return a pointer, and will never force you to maintain a pointer. All the API calls in KingDB return objects, and therefore the memory, file descriptors and other resources used by those objects, will all be released when the objects go out of scope.

###The Status class

The `Status` class is how KingDB deals with errors. All the methods in the KingDB API return a `Status` object. KingDB does not throw exceptions. Exceptions may still be thrown due to erroneous system calls, but KingDB does not catch them, because if they are thrown, it means that KingDB should die anyway.

The `Status` class allows you to test if an error occurred, and if so, to print the relevant error message:

    kdb::Status s = ...;
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Opening a database

    #include <kingdb/kdb.h>

    // Create a new database, which will be stored at the path "/tmp/mydb"
    kdb::DatabaseOptions db_options;
    kdb::Database db(db_options, "/tmp/mydb");
    kdb::Status s = db.Open();
    if (!s.IsOK()) cerr << s.ToString() << endl;
    db.Close(); // optional

###Reading, writing, and deleting an entry

    kdb::Status s;
    kdb::WriteOptions write_options;
    s = db.Put(write_options, “key1", "value1");
    if (!s.IsOK()) cerr << s.ToString() << endl;

    kdb::ReadOptions read_options;
    std::string value_out;
    s = db.Get(read_options, “key1", &value_out);
    if (!s.IsOK()) cerr << s.ToString() << endl;

    s = db.Delete("key1");
    if (!s.IsOK()) cerr << s.ToString() << endl;

**IMPORTANT:** If you need to store and retrieve entries larger than 1MB, read carefully the section about the [multipart API](#6-multipart-api).

###Syncing writes

You can sync writes to the secondary storage by setting the `sync` parameter in `WriteOptions`, which is false by default:

    kdb::WriteOptions write_options;
    write_options.sync = true;
    kdb::Status s = db.Put(write_options, “key1", "value1");
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Verifying checksums

A unique checksum is stored with each entry when it is persisted to secondary storage. By default, these checksums are not verified, but you can choose to verify these checksums when reading entries, by setting the `verify_checksums` parameter in `ReadOptions`, which is false by default:

    kdb::ReadOptions read_options;
    read_options.verify_checksums = true;
    std::string value_out;
    s = db.Get(read_options, “key1", &value_out);
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Closing a database

You can just let the database object go out of scope, which will close it. If you need to access databases with a pointer, deleting the pointer will close the database.

    // Example 1: Explicitly closing the database (not required)
    kdb::DatabaseOptions db_options;
    kdb::Database db(db_options, "mydb");
    kdb::Status s = db.Open();
    if (!s.IsOK()) cerr << s.ToString() << endl;
    db.Close();

    // Example 2: The destructor of Database will call Close() when db goes out of scope
    {
      kdb::DatabaseOptions db_options;
      kdb::Database db(db_options, "mydb");
      kdb::Status s = db.Open();
      if (!s.IsOK()) cerr << s.ToString() << endl;
    }

    // Example 3: When using a pointer
    kdb::DatabaseOptions db_options;
    kdb::Database* db = new Database(db_options, "mydb");
    kdb::Status s = db->Open();
    if (!s.IsOK()) cerr << s.ToString() << endl;
    delete db; // the destructor of Database will call Close()
    
**IMPORTANT:** Never create a database with the new operator, unless you really need pointer semantics.

###Compression

Compression is enabled by default, using the [LZ4 algorithm](https://github.com/Cyan4973/lz4). The compression option affects the behavior of an entire database: there is no option to compress some entries and keep the other uncompressed, it’s all or nothing. The compression parameter can be `kNoCompression` or `kLZ4Compression`. For example, the following code creates a database with compression disabled:

    kdb::DatabaseOptions db_options;
    db_options.compression = kNoCompression;
    kdb::Database db(db_options, "mydb");
    db.Open();

###Snapshots

You can get a read-only, consistent view of the database using a `Snapshot`:

    kdb::Snapshot snapshot = db.NewSnapshot();
    std::string value_out;
    kdb::Status s = snapshot.Get("key1", &value_out);
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Database and Snapshot interface

You can use the KingDB abstract class if you want pass either a `Database` or a `Snapshot`:

    kdb::DatabaseOptions db_options;
    kdb::Database db(db_options, "mydb");
    kdb::Status s = db.Open();
    if (!s.IsOK()) cerr << s.ToString() << endl;
    kdb::Snapshot snapshot = db.NewSnapshot();
    kdb::KingDB* mydb;

    if (condition) {
      mydb = &db;
    } else {
      mydb = &snapshot;
    }

    std::string value_out;
    kdb::Status s = mydb->Get("key1", &value_out);
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Iterators

Iterating over all the entries of a database can be done with the `Iterator` class.

    kdb::Iterator it = db.NewIterator();
    for(it.Begin(); it.IsValid(), it.Next()) {
        kdb::ByteArray key, value;
        key = it.GetKey();
        value = it.GetValue();
    }

###Working with the ByteArray class

    kdb::ByteArray key, value;

    // The data of a ByteArray can be transformed into std::string
    std::string key_str = key.ToString();

    // The data of a ByteArray can also be accessed as a C char array
    char* data = value.data();
    uint64_t size = value.size();
    for (auto i = 0; i < size; ++i) {
        do_something(data[i]);
    }

More information can be found in the [ByteArray section](#5-the-bytearray-class).

###Compaction

You can trigger a compaction process to compact all the data and make the database smaller.

    kdb::Status s = db.Compact();
    if (!s.IsOK()) cerr << s.ToString() << endl;

### Flushing and Syncing a database

You can force all KingDB's internal buffers to be flushed and synced to disk.

    kdb::Status s = db.Flush();
    if (!s.IsOK()) cerr << s.ToString() << endl;


##5. The ByteArray class

The `ByteArray` class allows to abstract the access to arbitrary arrays of bytes. The array can be allocated memory, a memory-mapped file, a shared memory, etc., it will all be transparent through the use of `ByteArray`.

    kdb::ByteArray ba;
    char*    mydata = ba.data(); // char* to the memory location
    uint64_t mysize = ba.size(); // size of the data found at the pointed memory location

`ByteArray` objects can be assigned, returned, and passed by value. Inside, a reference counter guarantees that the resources they hold will stay alive for as long as needed.

###Deep-copy ByteArray

The deep-copy `ByteArray` will allocate memory and copy the memory buffer it was passed.

    char* mybuffer = new char[1024];
    FillWithRandomContent(mybuffer, 1024);
    kdb::ByteArray ba = kdb::NewDeepCopyByteArray(buffer, 1024);
    delete[] mybuffer;
    ba.data(); // 'ba' holds its own copy of the data, so the data
               // is still reachable even though 'mybuffer' was deleted.

###Shallow-copy ByteArray

The shallow-copy `ByteArray` will become the owner of the memory address it was passed.

    char* mybuffer = new char[1024];
    FillWithRandomContent(mybuffer, 1024);
    kdb::ByteArray ba = kdb::NewShallowCopyByteArray(mybuffer, 1024);
    ba.data() // 'ba' now owns the allocated memory pointed by 'mybuffer'.
              // When 'ba' will be destroyed, it will release that memory.

###Memory-mapped ByteArray

If you want to read data from a file and used it as a `ByteArray`, you can simply let a `ByteArray` mmap() that file for you.

    std::string filepath("/tmp/myfile");
    uint64_t filesize = 1024;
    kdb::ByteArray ba = kdb::NewMmappedByteArray(filepath, filesize);

###Pointer ByteArray

The pointer `ByteArray` will hold a pointer to a memory location, but will not own it. If that memory location happens to be destroyed before the `ByteArray` is accessed, the program will likely crash due to a memory access violation error. The pointer `ByteArray` is very useful when high performance is needed as it doesn't need any memory allocation or system calls, but you need to use it with care.

    char* mybuffer = new char[1024];
    FillWithRandomContent(mybuffer, 1024);
    kdb::ByteArray ba = kdb::NewPointerByteArray(buffer, 1024);
    delete[] mybuffer; // Wrong: the delete will work, but any subsequent access to 'ba'
                       // is likely to make the program crash, because it is a pointer
                       // ByteArray, it does not own the memory it points to.

##6. Multipart API

###You can write an entry in multiple parts.

    int total_size = 1024 * 1024 * 128; // 128MB
    char buffer[1024 * 1024 * 128];
    kdb::MultipartWriter mp_writer = db_->NewMultipartWriter(write_options, key, total_size);

    int step = 1024 * 64; // 64KB steps
    for (auto i = 0; i < total_size; i += step) {
      kdb::ByteArray value = kdb::NewDeepCopyByteArray(buffer + i, step);
      kdb::Status s = mp_writer.PutPart(value);
      if (!s.IsOK()) {
        cerr << s.ToString() << endl;
        break;
      }
    }

###Multipart entries are read in multiple parts too.

    kdb::ReadOptions read_options;
    kdb::MultipartReader mp_reader = db_->NewMultipartReader(read_options, key);
    for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
      kdb::ByteArray part;
      kdb::Status s = mp_reader.GetPart(&part);
      if (!s.IsOK()) {
        cerr << s.ToString() << endl;
        break;
      }
      do_something(part);
    }
    kdb::Status s = mp_reader.GetStatus();
    if (!s.IsOK()) cerr << s.ToString() << endl;

###Multipart entries can be read in Iterators:

    for (it.Begin(); it.IsValid(); it.Next()) {
      kdb::ByteArray it.GetKey();
      kdb::MultipartReader mp_reader = it.GetMultipartValue();
      for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
        kdb::ByteArray part;
        kdb::Status s = mp_reader.GetPart(&part);
        if (!s.IsOK()) {
          cerr << s.ToString() << endl;
          break;
        }
      }
      kdb::Status s = mp_reader.GetStatus();
      if (!s.IsOK()) cerr << s.ToString() << endl;
    }

###Using the right interface

Currently, all entries larger an 1MB must be read with the multipart API. Why 1MB? It is a totally arbitrary size. Below 1MB, the `Get()` method of `Database` will allocate memory and fill that memory with the correct data for the value of the entry, taking care of the decompression if needed. Above 1MB, `Get()` will refuse to return the value of the entry, because it is possible that the value is just too big to fit in memory, thus the checks prevents KingDB from crashing. In that case, KingDB forces the user to use the multipart API. Again, the 1MB value is completely arbitrary: it is just a rule of thumb.  

If you are unsure whether or not your entries are multipart, you can either check the returning Status of Get() and adapt your code appropriately, or just use the multipart API for all your entries. Indeed, *all* entries can be read using the multipart API, even the ones that were not stored with the multipart API.

Thus if you don’t know if some of your entries are larger than 1MB, the first solution is check the return of `Get()`:

    kdb::ReadOptions read_options;
    std::string value_out;
    kdb::Status s = db.Get(read_options, key, &value_out);

    if (s.IsOK()) {
      do_something(value_out);
    } else if (s.IsMultipartRequired()) {
      kdb::MultipartReader mp_reader = db_->NewMultipartReader(read_options, key);
      for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
        kdb::ByteArray part;
        kdb::Status s = mp_reader.GetPart(&part);
        if (!s.IsOK()) {
          cerr << s.ToString() << endl;
          break;
        }
        do_something(part);
      }
      kdb::Status s = mp_reader.GetStatus();
      if (!s.IsOK()) {
        cerr << s.ToString() << endl;
      }
    } else {
      cerr << s.ToString() << endl;
    }

and the second solution is just to read all entries with the multipart API, which reduces the code:

    kdb::ReadOptions read_options;
    kdb::MultipartReader mp_reader = db_->NewMultipartReader(read_options, key);
    for (mp_reader.Begin(); mp_reader.IsValid(); mp_reader.Next()) {
      kdb::ByteArray part;
      kdb::Status s = mp_reader.GetPart(&part);
      if (!s.IsOK()) {
        cerr << s.ToString() << endl;
        break;
      }
      do_something(part);
    }
    kdb::Status s = mp_reader.GetStatus();
    if (!s.IsOK()) cerr << s.ToString() << endl;

##7. Logging with Syslog

###Selecting a log level

All the logging goes through [Syslog](http://en.wikipedia.org/wiki/Syslog), a protocol for message logging on Unix-based operating systems. The logging modules of KingDB and KingServer use Syslog to log activity and errors, and let the Syslog server on the machine handle storage and log rotation.

KingDB emits log messages with different priority levels, following most the priority levels of Syslog:

- silent: all logging is turned off
- emerg: system is unusable, imminent crash
- alert: error event, immediate action required
- crit: error event, immediate action required
- error: error event, action is required but is not urgent
- warn: events that can be harmful if no action is taken
- notice: unusual events, but no immediate action required
- info: normal operation events, no action required
- debug: events used for debugging, no action required
- trace: fine-grained events used for debugging, no action required

The default level is "info", and you can select the level of logging that you want with the kdb::Logger interface. For example, the following code set the logging level to “emerg”:

    kdb::Logger::set_current_level("emerg");

###Dedicated log file

By default, the log message will go to /var/log/system.log. You can also configure Syslog to store the KingDB and KingServer log messages to a dedicated log file on the machine. Below are examples of how to configure a Ubuntu server or a Mac OS X system to log all the messages emitted by KingDB to a dedicated file at the path /var/log/kingdb.log.

####On Ubuntu:

1. Open the rsyslog configuration file:

        $ sudo vim /etc/rsyslog.d/50-default.conf

2. Add a filter at the top of the file:

        :syslogtag, contains, "kingdb" /var/log/kingdb.log
        & ~

3. Restart rsyslog

        $ sudo service rsyslog restart

####On Mac OS X (using the [FreeBSD Syslog configuration](https://www.freebsd.org/doc/handbook/configtuning-syslog.html)):

1. Open the syslog configuration file:

        $ sudo vim /etc/syslog.conf

2. Add a filter at the top:

        !kingdb
        *.* /var/log/kingdb.log
        !*

3. Restart syslogd:

        $ sudo killall -HUP syslog syslogd

4. If the restart command above does not work, try this:

        $ sudo launchctl unload /System/Library/LaunchDaemons/com.apple.syslogd.plist
        $ sudo launchctl load /System/Library/LaunchDaemons/com.apple.syslogd.plist

##8. The KingServer network server

###What is KingServer?

KingServer is a server application that embeds KingDB and implements the Memcached protocol. It allows you to access your data through a network interface using whatever programming language you want. Use any Memcached client for the programming language that you want to use, point it to your KingServer instance, and start sending data in. It's really that simple!

Note that the current version of KingServer, version 0.9.0, implements only a subset of the operations of the Memcached protocol, which are: GET, SET, and DELETE. If you want more details about this protocol, you can refer to the [Memcached protocol specifications](https://github.com/memcached/memcached/blob/master/doc/protocol.txt). In addition, keep in mind that KingServer is not a distributed system: the data lives in a single machine. You can backup your data simply by setting up a periodic rsync between the directory where your KingDB database is stored, and your backup machine.

###How to run KingServer

The installation process described in the [installation section](#3-how-do-i-compile-my-programs-with-kingdb) will also install KingServer for you.

To list all the available options, use the `--help` parameter:

    $ kingserver --help

To start a server, the only required parameter is the location where your want to store your data, specified by the `--db.path` parameter. The following command will run KingServer as a daemon process in background, which will listen to the port 3490 (default Memcached port), and will store the data in the path /tmp/mydb:

    $ kingserver --db.path=/tmp/mydb

When you are done, you can stop the daemon by sending it a SIGTERM (15):

    $ pkill -f kingserver # will send SIGTERM to all processes whose name match 'kingserver'

For rapid testing and prototyping, you can also prevent KingServer from running as a daemon, and run it in foreground. You can also redirect the logging to stderr to monitor what is going on inside:

    $ kingserver --foreground --log.target=stderr --db.path=/tmp/mydb 
    2015/03/29-13:49:33.687759 0x7fff78663310 KingServer Daemon has started

When you are done, you can stop the daemon with CTRL+C:

    2015/03/29-13:54:02.623910 0x7fff78663310 KingServer Received signal [2]
    2015/03/29-13:54:02.627277 0x7fff78663310 KingServer Daemon has stopped

###Implementing a client to reach KingServer

The Memcached project keeps a [list of Memcached clients](https://code.google.com/p/memcached/wiki/Clients) for many programming languages. If you don't find your language in there, a simple Google search will find you a client.

Imagine that your client code is in Python, and that you pick the Memcached package for Python called `pylibmc`. A test client program would look like this:

    import pylibmc
    client = pylibmc.Client(["127.0.0.1:3490"])
    print "Setting 'key1’ to 'value1'"
    client['key1'] = 'value1'
    print "Retrieving the value for 'key1':" + client['key1']

As long as your point the client to right IP and port where KingServer listens, you'll be able to access your data.

###Configuration files

It would be tedious to have to specify all the options on the command line all the time. KingServer allows you to use a configuration file to set values for all options. Such a file would look like this:

    # hash can be used to add comments
    server.size-buffer-recv          8kb
    server.interface.memcached-port  3490
    server.num-threads               500

    db.path                          /tmp/mydb
    db.write-buffer.size             256mb
    db.write-buffer.flush-timeout    5 seconds
    db.storage.hstable-size          512mb
    db.compaction.force-interval     30000ms

For the data size parameters, such as `db.storage.hstable-size`: the default unit is the byte, but any other human-readable size units can also be specified. In the example configuration above, `db.storage.hstable-size` is set to 256mb, and the configuration manager of KingDB will convert that to bytes for you, and accepts both in lowercase or uppercase. The accepted size units are: b, byte, bytes, kb, mb, gb, tb, pb.

For the time-related parameters, such as `db.write-buffer.flush-timeout`, the default unit is the millisecond, but any other human-readable time units can also be specified. The accepted time units are: ms, millisecond, milliseconds, s, second, seconds, minute, minutes, hour, hours.

If the `--configfile` parameter is specified, KingServer will use the configuration file at that path. If no configuration file is specified, KingServer will look for one at the path `./kingdb.conf`, and `/etc/kingdb.conf`. If no file is found, the default values of all parameters will be used.

    $ kingserver --db.path /tmp/mydb --configfile /tmp/kingdb.conf

For a complete list of all the available options, you can use the `--help` parameter:

    $ kingserver --help

These options match exactly the options provided by the `ServerOptions` and `DatabaseOptions` classes, documented in the [Options section](#9-options).

##9. Options

###ReadOptions

`verify_checksums`  
It is set to false by default, and when set to true, the reads will verify the checksums and return an error when a checksum mismatch is detected.

###WriteOptions

`sync`  
It is set to false by default, and when set to true, the writes will be synced to secondary storage by calling fdatasync() on the file descriptor internally.

###DatabaseOptions

`db.create-if-missing`  
Will create the database if it does not already exists.  
Default value: True (Boolean)

`db.error-if-exists`  
Will exit if the database already exists.  
Default value: False (Boolean)

`db.incoming-rate-limit`  
Limit the rate of incoming traffic, in bytes per second. Unlimited if equal to 0.  
Default value: 0 (Unsigned 64-bit integer)

`db.write-buffer.size`  
Size of the Write Buffer.  
Default value: 64MB (Unsigned 64-bit integer)

`db.write-buffer.flush-timeout`  
The timeout after which the write buffer will flush its cache.  
Default value: 500 milliseconds (Unsigned 64-bit integer)

`db.write-buffer.mode`  
The mode with which the write buffer handles incoming traffic, can be 'direct' or 'adaptive'. With the 'direct' mode, once the Write Buffer is full other incoming Write and Delete operations will block until the buffer is persisted to secondary storage. The direct mode should be used when the clients are not subjects to timeouts. When choosing the 'adaptive' mode, incoming orders will be made slower, down to the speed of the writes on the secondary storage, so that they are almost just as fast as when using the direct mode, but are never blocking. The adaptive mode is expected to introduce a small performance decrease, but required for cases where clients timeouts must be avoided, for example when the database is used over a network.  
Default value: adaptive (String)

`db.storage.hstable-size`  
Maximum size a HSTable can have. Entries with keys and values beyond that size are considered to be large entries.  
Default value: 32MB (Unsigned 64-bit integer)

`db.storage.compression`  
Compression algorithm used by the storage engine. Can be 'disabled' or 'lz4'.  
Default value: lz4 (String)

`db.storage.hashing`  
Hashing algorithm used by the storage engine. Can be 'xxhash-64' or 'murmurhash3-64'.  
Default value: xxhash-64 (String)

`db.storage.minimum-free-space-accept-orders`  
Minimum free disk space required to accept incoming orders. It is recommended that for this value to be at least (2 x 'db.write-buffer.size' + 4 x 'db.hstable.maximum-size'), so that when the file system fills up, the two write buffers can be flushed to secondary storage safely and the survival-mode compaction process can be run.  
Default value: 192MB (Unsigned 64-bit integer)

`db.storage.maximum-part-size`  
The maximum part size is used by the storage engine to split entries into smaller parts -- important for the compression and hashing algorithms, can never be more than (2^32 - 1) as the algorihms used do not support sizes above that value.  
Default value: 1MB (Unsigned 64-bit integer)

`db.storage.inactivity-streaming`  
The time of inactivity after which an entry stored with the streaming API is considered left for dead, and any subsequent incoming parts for that entry are rejected.  
Default value: 60 seconds (Unsigned 64-bit integer)

`db.storage.statistics-polling-interval`  
The frequency at which statistics are polled in the Storage Engine (free disk space, etc.).  
Default value: 5 seconds (Unsigned 64-bit integer)

`db.compaction.force-interval`  
Duration after which, if no compaction process has been performed, a compacted is started. Set to 0 to disable.  
Default value: 5 minutes (Unsigned 64-bit integer)

`db.compaction.filesystem.free-space-required`  
Minimum free space on the file system required for a compaction process to be started.  
Default value: 128MB (Unsigned 64-bit integer)

`db.compaction.filesystem.survival-mode-threshold`  
If the free space on the file system is above that threshold, the compaction is in 'normal mode'. Below that threshold, the compaction is in 'survival mode'. Each mode triggers the compaction process for different amount of uncompacted data found in the database.  
Default value: 2GB (Unsigned 64-bit integer)

`db.compaction.filesystem.normal-batch-size`  
If the compaction is in normal mode and the amount of uncompacted data is above that value of 'normal-batch-size', then the compaction will start when the compaction conditions are checked.  
Default value: 1GB (Unsigned 64-bit integer)

`db.compaction.filesystem.survival-batch-size`  
If the compaction is in survival mode and the amount of uncompacted data is above that value of 'survival-batch-size', then the compaction will start when the compaction conditions are checked.  
Default value: 256MB (Unsigned 64-bit integer)


###ServerOptions

In addition to all the DatabaseOption, KingServer also accept a set of options to tune his behavior.

`configfile`  
Configuration file. If not specified, the path ./kingdb.conf and /etc/kingdb.conf will be tested.  
Default value: ./kingdb.conf (String)

`foreground`  
When set, the server will run as a foreground process. By default, the server runs as a daemon process.  
Default value: not set (Flag)

`db.path`  
Path where the database can be found or will be created.  
This parameter is *mandatory* (String)

`log.level`  
Level of the logging, can be: silent, emerg, alert, crit, error, warn, notice, info, debug, trace.  
Default value: info (String)

`log.target`  
Target of the logs, can be 'stderr' to log to stderr, or any custom string that will be used as the 'ident' parameter for syslog.  
Default value: kingdb (String)

`server.size-buffer-recv`  
Size of the buffer used to receive data from the network. Each thread of the server has one such buffer.  
Default value: 64KB (Unsigned 64-bit integer)

`server.listen-backlog`  
Size of the listen() backlog.  
Default value: 150 (Unsigned 32-bit integer)

`server.num-threads`  
Num of threads in the pool of workers.  
Default value: 150 (Unsigned 32-bit integer)

`server.interface.memcached-port`  
Port where the memcached interface will listen.  
Default value: 3490 (Unsigned 32-bit integer)


##10. Diving into the source code

*coming soon*
