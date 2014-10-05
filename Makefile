CC=g++
CFLAGS=-O3 -g -Wall -std=c++11 -stdlib=libc++ -Wc++11-extensions -Wc++1y-extensions -Wl,-no-as-needed -c
INCLUDES=-I/opt/local/include/ -I. -I./include/
LDFLAGS=-g -lprofiler
LDFLAGS_CLIENT=-g -L/opt/local/lib/ -lmemcached -lprofiler -fPIC
SOURCES=interface/kingdb.cc util/logger.cc util/status.cc network/server.cc buffer/buffer_manager.cc thread/event_manager.cc util/compressor.cc util/murmurhash3.cc util/xxhash.cc util/crc32c.cc util/lz4.cc util/hash.cc util/coding.cc unit-tests/testharness.cc
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
EXECUTABLE=server
CLIENT=client
CLIENT_EMB=client_emb
TEST_COMPRESSION=test_compression
TEST_DB=test_db
LIBRARY=kingdb.a

all: $(SOURCES) $(LIBRARY) $(EXECUTABLE) $(CLIENT) $(CLIENT_EMB) $(TEST_COMPRESSION) $(TEST_DB)

$(EXECUTABLE): $(OBJECTS) $(OBJECTS_MAIN)
	$(CC) $(LDFLAGS) $(OBJECTS) $(OBJECTS_MAIN) -o $@

$(CLIENT): $(OBJECTS) $(OBJECTS_CLIENT)
	$(CC) $(LDFLAGS_CLIENT) $(OBJECTS) $(OBJECTS_CLIENT) -o $@

$(CLIENT_EMB): $(OBJECTS) $(OBJECTS_CLIENT_EMB)
	$(CC) $(LDFLAGS_CLIENT) $(OBJECTS) $(OBJECTS_CLIENT_EMB) -o $@
#	$(CC) $(LDFLAGS_CLIENT) $(OBJECTS_CLIENT_EMB) $(LIBRARY) -o $@

$(TEST_COMPRESSION): $(OBJECTS) $(OBJECTS_TEST_COMPRESSION)
	$(CC) $(LDFLAGS_CLIENT) $(OBJECTS) $(OBJECTS_TEST_COMPRESSION) -o $@

$(TEST_DB): $(OBJECTS) $(OBJECTS_TEST_DB)
	$(CC) $(LDFLAGS_CLIENT) $(OBJECTS) $(OBJECTS_TEST_DB) -o $@

$(LIBRARY): $(OBJECTS)
	rm -f $@
	ar -rs $@ $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	rm -f *~ .*~ *.o $(EXECUTABLE) $(CLIENT) $(CLIENT_EMB) $(TEST_COMPRESSION) $(LIBRARY)
	rm -f buffer/*.o include/*.o interface/*.o network/*.o storage_engine/*.o thread/*.o unit-tests/*.o util/*.o
	rm -f buffer/*~ include/*~ interface/*~ network/*~ storage_engine/*~ thread/*~ unit-tests/*~ util/*~

