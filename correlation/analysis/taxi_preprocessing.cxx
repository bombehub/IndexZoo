#include <fstream>
#include <iostream>

#include "preprocessing.h"

int main() {

  std::string dst_filename = "/home/yingjun/Downloads/taxi.dat";
  FILE *dst_file = fopen(dst_filename.c_str(), "w+b");

  std::string src_filename = "/home/yingjun/Downloads/yellow_tripdata_2017-01.csv";
  std::ifstream src_file(src_filename);
  std::string line;
  std::vector<std::string> tokens;
  size_t count = 0;
  while (src_file.good()) {
    getline(src_file, line);

    if (count > 1) {
      tokens.clear();
      tokenize(line, tokens);
      // assert(tokens.size() == 17);
      if (tokens.size() != 17) {
        std::cout << line << std::endl;
        continue;
      }

      uint64_t distance = uint64_t(atof(tokens.at(4).c_str()) * 100);
      uint64_t fare = uint64_t(atof(tokens.at(10).c_str()) * 100);
      uint64_t total = uint64_t(atof(tokens.at(16).c_str()) * 100);

      // std::cout << distance << " " << fare << " " << total << std::endl;

      fwrite(&distance, sizeof(distance), 1, dst_file);
      fwrite(&fare, sizeof(fare), 1, dst_file);
      fwrite(&total, sizeof(total), 1, dst_file);
    }
    // if (count > 5) { break; }
    ++count;
  }

  src_file.close();
  fclose(dst_file);

}