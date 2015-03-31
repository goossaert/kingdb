#include <kingdb/kdb.h>

// Compiling:
// - LLVM:
// $ g++ -std=c++11 -I/usr/local/include/kingdb -lkingdb kingdb_user.cc -o kingdb_user
// - GCC:
// $ g++ -std=c++11 -I/usr/local/include/kingdb kingdb_user.cc -o kingdb_user -Wl,--no-as-needed -L/usr/local/lib -lpthread -lkingdb 

int main() {
  kdb::Status s;
  kdb::DatabaseOptions db_options;
  kdb::Database db(db_options, "mylocaldb");
  s = db.Open();
  if (!s.IsOK()) {
    fprintf(stderr, "Could not open the database: %s\n", s.ToString().c_str());
    exit(1);
  }

  std::string key1("key1");
  std::string key2("key2");
  std::string value1("value1");
  std::string value2("value2");

  kdb::WriteOptions write_options;
  s = db.Put(write_options, key1, value1);
  s = db.Put(write_options, key2, value2);

  kdb::ReadOptions read_options;
  int num_count_valid = 0;
  std::string out_str;
  s = db.Get(read_options, key1, &out_str);
  if (s.IsOK() && out_str == "value1") num_count_valid += 1;

  s = db.Get(read_options, key2, &out_str);
  if (s.IsOK() && out_str == "value2") num_count_valid += 1;

  if (num_count_valid == 2) {
    printf("Data successfully stored and retrieved\n"); 
  } else {
    printf("An error occurred\n"); 
  }

  return 0;
}
