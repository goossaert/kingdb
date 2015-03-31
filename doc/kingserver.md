Documentation of KingServer v0.9.0
==============================

**IMPORTANT:** This document is still a work in progress.

##Table of Contents

**[1. What is KingServer?](#1-what-is-kingserver)**  
**[2. How to install KingServer](#2-how-to-install-kingserver)**  
**[3. How to run KingServer?](#4-how-to-run-kingserver)**  
**[4. Configuration files](#4-configuration-files)**  
**[5. Logging with Syslog](#5-logging-with-syslog)**  
**[6. Options](#6-options)**  


##1. What is KingServer?

KingServer is a server application that embeds KingDB and implements the Memcached protocol. It allows you to access your data through a network interface using whatever programming language you want. Use any Memcached client for the programming language that you want to use, point it to your KingServer instance, and start sending data in. It's really that simple!

Note that the current version of KingServer, version 0.9.0, implements only a subset of the operations of the Memcached protocol, which are: GET, SET, and DELETE. If you want more details about this protocol, you can refer to the [Memcached protocol specifications](https://github.com/memcached/memcached/blob/master/doc/protocol.txt). In addition, keep in mind that KingServer is not a distributed system: the data lives in a single machine. You can backup your data simply by setting up a periodic rsync between the directory where your KingDB database is stored, and your backup machine.

For more information about KingDB, check out the [KingDB documentation](kingdb.md).


##2. How to install KingServer?

The installation process is the same as for KingDB.

KingServer has no external dependencies and has been tested on:

- Mac OS X 10.9.5 with Apple LLVM version 6.0 (clang-600.0.51)
- Linux Ubuntu 14.04 x64 with GCC 4.9.2
- Linux CentOS 6.5 x86\_64 with GCC 4.9.2

Because KingServer uses C++11, you need GCC version 4.9.2 or greater. The following commands will compile KingServer and will install the `kingserver` program.

    $ tar zxvf kingdb.tar.gz
    $ cd kingdb
    $ make
    $ sudo make install

##3. How to run KingServer?

To start a server, the only required parameter is the location where your want to store your data, specified by the `--db.path` parameter. The following command will run KingServer as a daemon process in background, which will listen to the port 11211 (default Memcached port), and will store the data in the path /tmp/mydb:

    $ kingserver --db.path=/tmp/mydb

When you are done, you can stop the daemon by sending it a SIGTERM (15):

    $ pkill -f kingserver # will send SIGTERM to all processes whose name match 'kingserver'

For rapid testing and prototyping, you can also prevent KingServer from running as a daemon, and run it in foreground. You can also redirect the logging to stderr to monitor what is going on inside:

    $ kingserver --foreground --log.target=stderr --db.path=/tmp/mydb 
    2015/03/29-13:49:33.687759 0x7fff78663310 KingServer Daemon has started

When you are done, you can stop the daemon with CTRL+C:

    2015/03/29-13:54:02.623910 0x7fff78663310 KingServer Received signal [2]
    2015/03/29-13:54:02.627277 0x7fff78663310 KingServer Daemon has stopped

##4. Implementing a network client to reach KingServer

The Memcached project keeps a [list of Memcached clients](https://code.google.com/p/memcached/wiki/Clients) for many programming languages. If you don't find your language in there, a simple Google search will find you a client.

Imagine that your client code is in Python, and that you pick the Memcached package for Python called `pylibmc`. A test client program would look like this:

    import pylibmc
    client = pylibmc.Client(["127.0.0.1:11211"])
    print "Setting 'key1' to 'value1'"
    client['key1'] = 'value1'
    print "Retrieving the value for 'key1':" + client['key1']

As long as your point the client to right IP and port where KingServer listens, you'll be able to access your data.

##4. Configuration files

It would be tedious to have to specify all the options on the command line all the time. KingServer allows you to use a configuration file to set values for all options. Such a file would look like this:

    # hash can be used to add comments
    server.size-buffer-recv          8kb
    server.interface.memcached-port  11211
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

This list is reproduced below in the [Options section](#6-options).


##5. Logging with Syslog

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


##6. Options

###Server Options

Options that alter the behavior of the KingServer network server.

`--configfile`  
Configuration file. If not specified, the path ./kingdb.conf and /etc/kingdb.conf will be tested.  
Default value: ./kingdb.conf (String)

`--foreground`  
When set, the server will run as a foreground process. By default, the server runs as a daemon process.  
Default value: not set (Flag)

`--log.level`  
Level of the logging, can be: silent, emerg, alert, crit, error, warn, notice, info, debug, trace.  
Default value: info (String)

`--log.target`  
Target of the logs, can be 'stderr' to log to stderr, or any custom string that will be used as the 'ident' parameter for syslog.  
Default value: kingdb (String)

`--server.size-buffer-recv`  
Size of the buffer used to receive data from the network. Each thread of the server has one such buffer.  
Default value: 64KB (Unsigned 64-bit integer)

`--server.listen-backlog`  
Size of the listen() backlog.  
Default value: 150 (Unsigned 32-bit integer)

`--server.num-threads`  
Num of threads in the pool of workers.  
Default value: 150 (Unsigned 32-bit integer)

`--server.interface.memcached-port`  
Port where the memcached interface will listen.  
Default value: 11211 (Unsigned 32-bit integer)

###Database Options

With the following options, you can change the behavior of the KingDB database embedded in the KingServer process that you are running.

`--db.path`  
Path where the database can be found or will be created.  
This parameter is *mandatory* (String)

`--db.create-if-missing`  
Will create the database if it does not already exists.  
Default value: True (Boolean)

`--db.error-if-exists`  
Will exit if the database already exists.  
Default value: False (Boolean)

`--db.incoming-rate-limit`  
Limit the rate of incoming traffic, in bytes per second. Unlimited if equal to 0.  
Default value: 0 (Unsigned 64-bit integer)

`--db.write-buffer.size`  
Size of the Write Buffer.  
Default value: 64MB (Unsigned 64-bit integer)

`--db.write-buffer.flush-timeout`  
The timeout after which the write buffer will flush its cache.  
Default value: 500 milliseconds (Unsigned 64-bit integer)

`--db.write-buffer.mode`  
The mode with which the write buffer handles incoming traffic, can be 'direct' or 'adaptive'. With the 'direct' mode, once the Write Buffer is full other incoming Write and Delete operations will block until the buffer is persisted to secondary storage. The direct mode should be used when the clients are not subjects to timeouts. When choosing the 'adaptive' mode, incoming orders will be made slower, down to the speed of the writes on the secondary storage, so that they are almost just as fast as when using the direct mode, but are never blocking. The adaptive mode is expected to introduce a small performance decrease, but required for cases where clients timeouts must be avoided, for example when the database is used over a network.
Default value: adaptive (String)

`--db.storage.hstable-size`  
Maximum size a HSTable can have. Entries with keys and values beyond that size are considered to be large entries.  
Default value: 32MB (Unsigned 64-bit integer)

`--db.storage.compression`  
Compression algorithm used by the storage engine. Can be 'disabled' or 'lz4'.  
Default value: lz4 (String)

`--db.storage.hashing`  
Hashing algorithm used by the storage engine. Can be 'xxhash-64' or 'murmurhash3-64'.  
Default value: xxhash-64 (String)

`--db.storage.minimum-free-space-accept-orders`  
Minimum free disk space required to accept incoming orders. It is recommended that for this value to be at least (2 x 'db.write-buffer.size' + 4 x 'db.storage.hstable-size'), so that when the file system fills up, the two write buffers can be flushed to secondary storage safely and the survival-mode compaction process can be run.  
Default value: 192MB (Unsigned 64-bit integer)

`--db.storage.maximum-part-size`  
The maximum part size is used by the storage engine to split entries into smaller parts -- important for the compression and hashing algorithms, can never be more than (2^32 - 1) as the algorihms used do not support sizes above that value.  
Default value: 1MB (Unsigned 64-bit integer)

`--db.storage.inactivity-streaming`  
The time of inactivity after which an entry stored with the streaming API is considered left for dead, and any subsequent incoming parts for that entry are rejected.  
Default value: 60 seconds (Unsigned 64-bit integer)

`--db.storage.statistics-polling-interval`  
The frequency at which statistics are polled in the Storage Engine (free disk space, etc.).  
Default value: 5 seconds (Unsigned 64-bit integer)

`--db.compaction.force-interval`  
Duration after which, if no compaction process has been performed, a compacted is started. Set to 0 to disable.  
Default value: 5 minutes (Unsigned 64-bit integer)

`--db.compaction.filesystem.free-space-required`  
Minimum free space on the file system required for a compaction process to be started.  
Default value: 128MB (Unsigned 64-bit integer)

`--db.compaction.filesystem.survival-mode-threshold`  
If the free space on the file system is above that threshold, the compaction is in 'normal mode'. Below that threshold, the compaction is in 'survival mode'. Each mode triggers the compaction process for different amount of uncompacted data found in the database.  
Default value: 2GB (Unsigned 64-bit integer)

`--db.compaction.filesystem.normal-batch-size`  
If the compaction is in normal mode and the amount of uncompacted data is above that value of 'normal-batch-size', then the compaction will start when the compaction conditions are checked.  
Default value: 1GB (Unsigned 64-bit integer)

`--db.compaction.filesystem.survival-batch-size`  
If the compaction is in survival mode and the amount of uncompacted data is above that value of 'survival-batch-size', then the compaction will start when the compaction conditions are checked.  
Default value: 256MB (Unsigned 64-bit integer)


