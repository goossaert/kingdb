#include "util/compressor.h"
#include "util/lz4.h"


char* MakeValue(const std::string& key, int size_value) {
  int size_key = key.size();
  char *str = new char[size_value+1];
  str[size_value] = '\0';
  int i = 0;
  for (i = 0; i < size_value / size_key; i++) {
    memcpy(str + i*size_key, key.c_str(), size_key);
  }
  if (size_value % size_key != 0) {
    memcpy(str + i*size_key, key.c_str(), size_value % size_key);
  }
  return str;
}

int VerifyValue(const std::string& key, int size_value, const char* value) {
  int size_key = key.size();
  int i = 0;
  bool error = false;
  for (i = 0; i < size_value / size_key; i++) {
    if (memcmp(value + i*size_key, key.c_str(), size_key)) {
      std::string value2(value + i*size_key, size_key);
      printf("diff i:%d size:%d key:[%s], value:[%s]\n", i, size_key, key.c_str(), value2.c_str());
      error = true;
    }
  }
  if (size_value % size_key != 0) {
    if (memcmp(value + i*size_key, key.c_str(), size_value % size_key)) {
      std::string value2(value, size_value % size_key);
      printf("diff remainder size:%d key:[%s], value:[%s]\n", size_value % size_key, key.c_str(), value2.c_str());
      error = true;
    }
  }
  if (error) return -1;
  return 0;
}



int main() {
  kdb::CompressorLZ4 lz4;
  char* raw = nullptr;
  char *compressed = nullptr;
  uint64_t size_value = 442837;
  uint64_t size_compressed = 0;
  uint64_t size_chunk = 64*1024;
  uint64_t offset_chunk_compressed = 0;
  uint64_t SIZE_BUFFER = 1024*1024;

  std::string key("0x10c095000-0");
  raw = MakeValue(key, size_value);
  lz4.ResetThreadLocalStorage();
  compressed = new char[SIZE_BUFFER];

  auto num_chunks = size_value / size_chunk;
  char *comp;
  if (size_value % size_chunk) num_chunks += 1;
  for(auto chunk = 0; chunk < num_chunks; chunk++) {
    uint64_t size_chunk_current = 0;
    size_chunk_current = size_chunk;
    if (chunk == num_chunks - 1) {
      size_chunk_current = size_value % size_chunk;
    }
    fprintf(stderr, "step:%d size:%llu\n", chunk, size_chunk_current);

    offset_chunk_compressed = lz4.size_compressed();
    kdb::Status s = lz4.Compress(raw + chunk * size_chunk,
                                 size_chunk_current,
                                 &comp,
                                 &size_compressed);
    if (!s.IsOK()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      exit(-1);
    }

    fprintf(stderr, "step %d - %p size_compressed:%llu offset:%llu\n", chunk, comp, size_compressed, offset_chunk_compressed);
    memcpy(compressed + offset_chunk_compressed, comp, size_compressed);
    fprintf(stderr, "step %d - size_compressed:%llu\n", chunk, size_compressed);
  }

  size_compressed = lz4.size_compressed();

  fprintf(stderr, "--- stream compressed data (size:%llu):\n", size_compressed);
  for (auto i = 0; i < size_compressed; i++) {
    fprintf(stderr, "%c", compressed[i]);
  }
  fprintf(stderr, "\n--- done\n");


  char *uncompressed;
  uint64_t size_out;
  uint64_t size_out_total = 0;
  char *frame;
  uint64_t size_frame;
  int step = 0;
  char *uncompressed_full = new char[1024*1024];
  while(true) {
    kdb::Status s1 = lz4.Uncompress(compressed,
                                    size_compressed,
                                    &uncompressed,
                                    &size_out,
                                    &frame,
                                    &size_frame);
    fprintf(stderr, "stream uncompressed step: %d size:%llu\n", step, size_out);
    if (!s1.IsOK()) {
      fprintf(stderr, "%s\n", s1.ToString().c_str());
      break;
    }
    memcpy(uncompressed_full + size_out_total, uncompressed, size_out);
    size_out_total += size_out;
    step += 1;
  }
  fprintf(stderr, "stream uncompressed size: %llu\n", size_out_total);

  int ret = VerifyValue(key, size_value, uncompressed_full);
  if (ret == 0) {
    fprintf(stderr, "Verify(): ok\n");
  }

  return 0;

}
