#include <fstream>
#include <iostream>

#include "preprocessing.h"

int main() {

  std::string dst_filename = "/home/yingjun/Downloads/flights.dat";
  FILE *dst_file = fopen(dst_filename.c_str(), "w+b");

  std::string src_filename = "/home/yingjun/Downloads/flights.csv";
  std::ifstream src_file(src_filename);
  std::string line;
  std::vector<std::string> tokens;
  size_t count = 0;
  while (src_file.good()) {
    getline(src_file, line);

    if (count > 1) {
      tokens.clear();
      tokenize(line, tokens);

      if (tokens.size() < 18) {
        std::cout << line << std::endl;
        continue;
      }

      uint64_t elapsed_time = uint64_t(atof(tokens.at(15).c_str()));
      uint64_t air_time = uint64_t(atof(tokens.at(16).c_str()));
      uint64_t distance = uint64_t(atof(tokens.at(17).c_str()));

      // std::cout << elapsed_time << " " << air_time << " " << distance << std::endl;

      fwrite(&elapsed_time, sizeof(elapsed_time), 1, dst_file);
      fwrite(&air_time, sizeof(air_time), 1, dst_file);
      fwrite(&distance, sizeof(distance), 1, dst_file);
    }
    // if (count > 5) { break; }
    ++count;
  }

  std::cout << "count = " << count << std::endl;

  src_file.close();
  fclose(dst_file);

}