CC=g++
INCLUDES=-I/usr/local/include/ -I/opt/local/include/ -I. -I./include/
LDFLAGS=-g -L/usr/local/lib/ -L/opt/local/lib/ -lpthread
LDFLAGS_CLIENT=-g -L/usr/local/lib/ -L/opt/local/lib/ -lpthread -lmemcached -fPIC
SOURCES=interface/database.cc util/logger.cc util/status.cc util/debug.cc network/server.cc cache/write_buffer.cc algorithm/endian.cc algorithm/compressor.cc algorithm/murmurhash3.cc algorithm/xxhash.cc algorithm/crc32c.cc algorithm/lz4.cc algorithm/hash.cc algorithm/coding.cc unit-tests/testharness.cc
SOURCES_MAIN=network/server_main.cc
SOURCES_CLIENT=network/client_main.cc
SOURCES_CLIENT_EMB=unit-tests/client_embedded.cc
SOURCES_TEST_COMPRESSION=unit-tests/test_compression.cc
SOURCES_TEST_DB=unit-tests/test_db.cc
OBJECTS=$(SOURCES:.cc=.o)
OBJECTS_MAIN=$(SOURCES_MAIN:.cc=.o)
OBJECTS_CLIENT=$(SOURCES_CLIENT:.cc=.o)
OBJECTS_CLIENT_EMB=$(SOURCES_CLIENT_EMB:.cc=.o)
OBJECTS_TEST_COMPRESSION=$(SOURCES_TEST_COMPRESSION:.cc=.o)
OBJECTS_TEST_DB=$(SOURCES_TEST_DB:.cc=.o)
EXECUTABLE=kingserver
CLIENT_NETWORK=client_network
CLIENT_EMB=client_emb
TEST_COMPRESSION=test_compression
TEST_DB=test_db
LIBRARY=libkingdb.a
PREFIX=/usr/local
BINDIR=$(PREFIX)/bin
INCLUDEDIR=$(PREFIX)/include/kingdb
LIBDIR=$(PREFIX)/lib

CFLAGS=-std=c++11 -MMD -MP -c

all: CFLAGS += -O2
all: $(SOURCES) $(LIBRARY) $(EXECUTABLE) $(CLIENT_EMB) $(TEST_COMPRESSION) $(TEST_DB)

debug: CFLAGS += -DDEBUG -g
debug: LDFLAGS+= -lprofiler 
debug: $(SOURCES) $(LIBRARY) $(EXECUTABLE) $(CLIENT_EMB) $(TEST_COMPRESSION) $(TEST_DB)

client: CFLAGS += -O2
client: $(SOURCES) $(CLIENT_NETWORK)

client-debug: CFLAGS += -DDEBUG -g
client-debug: LDFLAGS_CLIENT += -lprofiler 
client-debug: $(SOURCES) $(CLIENT_NETWORK)

threadsanitize: CFLAGS += -DDEBUG -g -fsanitize=thread -O2 -pie -fPIC
threadsanitize: LDFLAGS += -pie -ltsan
threadsanitize: LDFLAGS_CLIENT += -pie -ltsan
threadsanitize: $(SOURCES) $(LIBRARY) $(EXECUTABLE) $(CLIENT_EMB) $(CLIENT_NETWORK) $(TEST_COMPRESSION) $(TEST_DB)

$(EXECUTABLE): $(OBJECTS) $(OBJECTS_MAIN)
	$(CC) $(OBJECTS) $(OBJECTS_MAIN) -o $@ $(LDFLAGS) 

$(CLIENT_NETWORK): $(OBJECTS) $(OBJECTS_CLIENT)
	$(CC) $(OBJECTS) $(OBJECTS_CLIENT) -o $@ $(LDFLAGS_CLIENT)

$(CLIENT_EMB): $(OBJECTS) $(OBJECTS_CLIENT_EMB)
	$(CC) $(OBJECTS) $(OBJECTS_CLIENT_EMB) -o $@ $(LDFLAGS)

$(TEST_COMPRESSION): $(OBJECTS) $(OBJECTS_TEST_COMPRESSION)
	$(CC) $(OBJECTS) $(OBJECTS_TEST_COMPRESSION) -o $@ $(LDFLAGS)

$(TEST_DB): $(OBJECTS) $(OBJECTS_TEST_DB)
	$(CC) $(OBJECTS) $(OBJECTS_TEST_DB) -o $@ $(LDFLAGS)

$(LIBRARY): $(OBJECTS)
	rm -f $@
	ar -rs $@ $(OBJECTS)

install: $(EXECUTABLE)
	install $(EXECUTABLE) $(BINDIR)
	cp libkingdb.a $(LIBDIR)
	rm -rf $(INCLUDEDIR)
	mkdir $(INCLUDEDIR) $(INCLUDEDIR)/util $(INCLUDEDIR)/thread $(INCLUDEDIR)/interface $(INCLUDEDIR)/storage $(INCLUDEDIR)/algorithm $(INCLUDEDIR)/cache
	cp algorithm/*.h $(INCLUDEDIR)/algorithm
	cp cache/*.h $(INCLUDEDIR)/cache
	cp include/kingdb/*.h $(INCLUDEDIR)
	cp interface/*.h $(INCLUDEDIR)/interface
	cp util/*.h $(INCLUDEDIR)/util
	cp thread/*.h $(INCLUDEDIR)/thread
	cp storage/*.h $(INCLUDEDIR)/storage

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	rm -f $(EXECUTABLE) $(CLIENT_NETWORK) $(CLIENT_EMB) $(TEST_COMPRESSION) $(TEST_DB) $(LIBRARY)
	find . -name \.*.*.swp* -type f -print0  | xargs -0 rm -f
	find . -name \*.d       -type f -print0  | xargs -0 rm -f
	find . -name \*.o       -type f -print0  | xargs -0 rm -f
	find . -name \*~        -type f -print0  | xargs -0 rm -f
	find . -name \*-e       -type f -print0  | xargs -0 rm -f

-include $(SOURCES:%.cc=%.d)
