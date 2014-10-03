#include "network/client.h"

void show_usage(char *program_name) {
  printf("Example: %s --host 127.0.0.1:3490 --num-threads 120 --write 10000 --remove 5000 --read 10000\n", program_name);
}

int main(int argc, char **argv) {
  if (argc == 1) {
    show_usage(argv[0]); 
    exit(0);
  }

  if (argc % 2 == 0) {
    std::cerr << "Error: invalid number of arguments" << std::endl; 
    show_usage(argv[0]); 
    exit(-1);
  }

  std::string host("");
  int num_threads = 0;
  int num_writes = 0;
  int num_removes = 0;
  int num_reads = 0;

  if (argc > 2) {
    for (int i = 1; i < argc; i += 2 ) {
      if (strcmp(argv[i], "--host" ) == 0) {
        host = "--SERVER=" + std::string(argv[i+1]);
      } else if (strcmp(argv[i], "--num-threads" ) == 0) {
        num_threads = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--write" ) == 0) {
        num_writes = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--remove" ) == 0) {
        num_removes = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--read" ) == 0) {
        num_reads = atoi(argv[i+1]);
      } else if (strcmp(argv[i], "--log-level" ) == 0) {
        if (kdb::Logger::set_current_level(argv[i+1]) < 0 ) {
          fprintf(stderr, "Unknown log level: [%s]\n", argv[i+1]);
          exit(-1); 
        }
      } else {
        fprintf(stderr, "Unknown parameter [%s]\n", argv[i]);
        exit(-1); 
      }
    }
  }

  if (host == "" || num_threads == 0) {
    fprintf(stderr, "Missing arguments\n");
    exit(-1); 
  }

  kdb::ThreadPool tp(num_threads);
  tp.Start();
  for (auto i = 0; i < num_threads; i++ ) {
    tp.AddTask(new kdb::ClientTask(host, num_writes, num_removes, num_reads));
  }
  tp.BlockUntilAllTasksHaveCompleted();
  return 0;
}

