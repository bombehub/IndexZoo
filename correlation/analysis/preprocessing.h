#include <sstream>
#include <string>
#include <vector>

static void tokenize(const std::string &input, std::vector<std::string> &output) {
  std::istringstream ss(input);
  std::string token;

  while(std::getline(ss, token, ',')) {
    output.push_back(token);
  }
}
