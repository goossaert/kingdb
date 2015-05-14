// Copyright (c) 2014, Emmanuel Goossaert. All rights reserved.
// Use of this source code is governed by the BSD 3-Clause License,
// that can be found in the LICENSE file.

#ifndef KINGDB_CONFIG_PARSER_H_
#define KINGDB_CONFIG_PARSER_H_

#include "util/debug.h"

#include <cinttypes>
#include <sys/stat.h>
#include <string.h>
#include <fstream>
#include <string>
#include <regex>
#include <vector>
#include <set>
#include <map>

#include "util/status.h"

// TODO: have short letter version of parameter names? ex: -l or --loglevel for the same parameter
// TODO: error if duplicate parameter is same scope (file or command-line)

namespace kdb {

class Parameter {
 public:
  std::string name;
  std::string description;
  std::string default_value;
  bool is_mandatory;
  virtual ~Parameter() {}
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) = 0;
  virtual std::string Type() = 0;
  uint64_t GetMultiplier(std::string str) {
    std::regex regex_number {"([\\d]+)[\\s]*([^\\s]*)"};
    std::smatch matches;
    if (!std::regex_search(str, matches, regex_number) || matches.size() != 3) {
      return 1;
    }
    std::string number(matches[1]);
    std::string unit(matches[2]);
    //fprintf(stderr, "num:%s, unit:[%s]\n", number.c_str(), unit.c_str());
    std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);

    if (unit == "") return 1;

    // Parsing space units
    if (unit == "b" || unit == "byte" || unit == "bytes") {
      return 1;
    } else if (unit == "kb") {
      return 1024;
    } else if (unit == "mb") {
      return 1024*1024;
    } else if (unit == "gb") {
      return (uint64_t)1024*(uint64_t)1024*1024;
    } else if (unit == "tb") {
      return (uint64_t)1024*1024*1024*1024;
    } else if (unit == "pb") {
      return (uint64_t)1024*1024*1024*1024*1024;

    // Parsing temporal units
    } else if (unit == "ms" || unit == "millisecond" || unit == "milliseconds") {
      return 1;
    } else if (unit == "s" || unit == "second" || unit == "seconds") {
      return 1000;
    } else if (unit == "minute" || unit == "minutes") {
      return 1000 * 60;
    } else if (unit == "hour" || unit == "hours") {
      return 1000 * 60 * 60;
    }
    return 0;
  }
};

class FlagParameter: public Parameter {
 public:
  bool* is_present;
  FlagParameter(const std::string& name_in, bool* is_present_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = "not set";
    is_present = is_present_in;
    *is_present = false;
  }
  virtual ~FlagParameter() {}
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) {
    *is_present = true;
    return Status::OK();
  }
  virtual std::string Type() { return "Flag"; }
};


class BooleanParameter: public Parameter {
 public:
  bool* state;
  BooleanParameter(const std::string& name_in, bool default_in, bool* is_present_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = default_in ? "True" : "False";
    state = is_present_in;
    *state = default_in;
  }
  virtual ~BooleanParameter() {}
  virtual Status Parse(const std::string& config, const std::string& value_in, const std::string& filepath, int line_number) {
    std::string value(value_in);
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    if (value == "true" || value == "1") {
      *state = true;
    } else if (value == "false" || value == "0") {
      *state = false;
    } else {
      std::string str_line_number = "Invalid value for boolean parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    return Status::OK();
  }
  virtual std::string Type() { return "Boolean"; }
};




class UnsignedInt32Parameter: public Parameter {
 public:
  uint32_t *ptr;
  UnsignedInt32Parameter(const std::string& name_in, const std::string default_in, uint32_t *ptr_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = default_in;
    ptr = ptr_in;
    Status s = Parse(name, default_in, "default-value", 0);
    if (!s.IsOK()) {
      fprintf(stderr, "Error: invalid default value for parameter [%s]\n", name.c_str());
      exit(1);
    }
  }
  virtual ~UnsignedInt32Parameter() {}
  virtual uint32_t Get() { return *ptr; }
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) {
    int num_scanned = sscanf(value.c_str(), "%u", ptr);
    if (num_scanned != 1) {
      std::string str_line_number = "Invalid value for unsigned 32-bit integer parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    uint64_t multiplier = GetMultiplier(value);
    if (multiplier == 0) {
      std::string str_line_number = "Invalid unit for parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    *ptr = *ptr * multiplier;
    return Status::OK();
  }
  virtual std::string Type() { return "Unsigned 32-bit integer"; }
};


class UnsignedInt64Parameter: public Parameter {
 public:
  uint64_t *ptr;
  UnsignedInt64Parameter(const std::string& name_in, const std::string& default_in, uint64_t *ptr_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = default_in;
    ptr = ptr_in;
    Status s = Parse(name, default_in, "default-value", 0);
    if (!s.IsOK()) {
      fprintf(stderr, "Error: invalid default value for parameter [%s]\n", name.c_str());
      exit(1);
    }
  }
  virtual ~UnsignedInt64Parameter() {}
  virtual uint64_t Get() { return *ptr; }
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) {
    int num_scanned = sscanf(value.c_str(), "%" PRIu64, ptr);
    if (num_scanned != 1) {
      std::string str_line_number = "Invalid value for unsigned 64-bit integer parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    uint64_t multiplier = GetMultiplier(value);
    if (multiplier == 0) {
      std::string str_line_number = "Invalid unit for parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    *ptr = *ptr * multiplier;
    return Status::OK();
  }
  virtual std::string Type() { return "Unsigned 64-bit integer"; }
};


class DoubleParameter: public Parameter {
 public:
  double *ptr;
  DoubleParameter(const std::string& name_in, const std::string& default_in, double *ptr_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = default_in;
    ptr = ptr_in;
    Status s = Parse(name, default_in, "default-value", 0);
    if (!s.IsOK()) {
      fprintf(stderr, "Error: invalid default value for parameter [%s]\n", name.c_str());
      exit(1);
    }
  }
  virtual ~DoubleParameter() {}
  virtual double Get() { return *ptr; }
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) {
    int num_scanned = sscanf(value.c_str(), "%lf", ptr);
    if (num_scanned != 1) {
      std::string str_line_number = "Invalid value for double-precision number parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
      return Status::IOError("ConfigParser", str_line_number);
    }
    return Status::OK();
  }
  virtual std::string Type() { return "Double-precision number"; }
};


class StringParameter: public Parameter {
 public:
  std::string *ptr;
  StringParameter(const std::string& name_in, const std::string& default_in, std::string* ptr_in, bool mandatory_in, const std::string& description_in) {
    name = name_in;
    is_mandatory = mandatory_in;
    description = description_in;
    default_value = default_in;
    ptr = ptr_in;
    *ptr = default_in;
  }
  virtual ~StringParameter() {}
  virtual std::string Get() { return *ptr; }
  virtual Status Parse(const std::string& config, const std::string& value, const std::string& filepath, int line_number) {
    *ptr = value;
    return Status::OK();
  }
  virtual std::string Type() { return "String"; }
};


class ConfigParser {
 public:
  ConfigParser()
      : error_if_unknown_parameters(true) {
  }

  ~ConfigParser() {
    for (size_t i = 0; i < parameters_.size(); i++) {
      delete parameters_[i];
    }
  }

  void AddParameter(Parameter* parameter) {
    parameters_.push_back(parameter);
    if (parameter->is_mandatory) {
      mandatories_.insert(parameter->name);
    }
  }

  void SetDefaultValue(const std::string name, const std::string default_value) {
    // This is used to change the default value of a parameter after
    // the parameter was added to the parser.
    auto it = parameters_.begin();
    for (; it != parameters_.end(); ++it) {
      if ((*it)->name == name) break;
    }
    if (it != parameters_.end()) {
      (*it)->default_value = default_value;
    }
  }

  bool FoundAllMandatoryParameters() {
    return (mandatories_.size() == 0); 
  }

  void PrintAllMissingMandatoryParameters() {
    if (mandatories_.size() == 0) return;
    fprintf(stderr, "Error: the following mandatory parameters are missing:\n");
    for (auto& name: mandatories_) {
      fprintf(stderr, "%s\n", name.c_str()); 
    }
  }

  int min_int(int a, int b) {
    return a < b ? a : b;
  }

  std::string AlignString(int margin, int column, std::string& str) {
    // Obviously not efficient, but simple and fast enough to format
    // the parameters so they can be displayed in PrintUsage()
    std::string str_aligned;
    size_t i = 0;
    while (i < str.size()) {
      size_t j_start = i + column;
      size_t j = (j_start <= str.size()) ? j_start : i;
      while (j > i) {
        if (str[j] == ' ') break;
        j--;
      }
      if (j <= i) j = str.size();
      for (int k = 0; k < margin; k++) str_aligned += " ";
      str_aligned += str.substr(i, j - i);
      if (j + 1 < str.size()) str_aligned += "\n";
      i = j + 1;
    }

    return str_aligned;
  }

  void PrintUsage() {
    int margin = 6;
    int column = 74;
    std::string str_margin = "";
    for (int k = 0; k < margin; k++) str_margin += " ";
    for (auto &p: parameters_) {
      fprintf(stdout, "  --%s:\n", p->name.c_str());
      std::string d_aligned = AlignString(margin, column, p->description);
      fprintf(stdout, "%s\n", d_aligned.c_str());
      if (mandatories_.find(p->name) == mandatories_.end()) {
        fprintf(stdout, "%sDefault value: %s (%s)\n", str_margin.c_str(), p->default_value.c_str(), p->Type().c_str());
      } else {
        fprintf(stdout, "%sThis parameter is *mandatory* (%s)\n", str_margin.c_str(), p->Type().c_str());
      }
      fprintf(stdout, "\n");
    }
  }

  void PrintMarkdown() {
    for (auto &p: parameters_) {
      fprintf(stdout, "`%s`  \n%s  \n", p->name.c_str(), p->description.c_str());
      if (mandatories_.find(p->name) == mandatories_.end()) {
        fprintf(stdout, "Default value: %s (%s)\n", p->default_value.c_str(), p->Type().c_str());
      } else {
        fprintf(stdout, "This parameter is *mandatory* (%s)\n", p->Type().c_str());
      }
      fprintf(stdout, "\n");
    }
  }

  Status LoadDefaultValues() {
    return ParseCommandLine(0, nullptr);
  }

  Status ParseCommandLine(int argc, char **argv) {
    std::map<std::string, Parameter*> parameters;
    for (auto& p: parameters_) {
      parameters[p->name] = p;
    }

    int i = 1;
    while (i < argc) {
      if (strncmp(argv[i], "--", 2) != 0) {
        if (error_if_unknown_parameters) {
          std::string msg = "Invalid parameter [" + std::string(argv[i]) + "]";
          return Status::IOError("ConfigReader::ReadCommandLine()", msg);
        } else {
          i++;
          continue;
        }
      }
      std::string argument_raw(argv[i]);
      std::string argument = argument_raw.substr(2);

      // Find '=' sign if any
      bool has_equal_sign = false;
      std::string value;
      int pos = argument.find('=');
      if (pos != -1) {
        value = argument.substr(pos+1);
        argument = argument.substr(0, pos);
        has_equal_sign = true;
        //fprintf(stderr, "argument:[%s] value:[%s]\n", argument.c_str(), value.c_str());
      }

      // Check if parameter exists
      if (parameters.find(argument) == parameters.end()) {
        if (error_if_unknown_parameters) {
          std::string msg = "Unknown parameter [" + argument + "]";
          return Status::IOError("ConfigReader::ReadCommandLine()", msg);
        } else {
          i++;
          continue;
        }
      }

      // Rejects flag parameters with equal signs
      bool is_flag_parameter = (dynamic_cast<FlagParameter*>(parameters[argument]) != nullptr);
      if (is_flag_parameter && has_equal_sign) {
        std::string msg = "The argument [" + std::string(argv[i]) + "] is of type FlagParameter and has an equal sign";
        return Status::IOError("ConfigReader::ReadCommandLine()", msg);
      }

      // No '=' sign, using next parameter as value
      if (!is_flag_parameter && !has_equal_sign) {
        if (i+1 >= argc) {
          std::string msg = "Missing value for parameter [" + std::string(argv[i]) + "]";
          return Status::IOError("ConfigReader::ReadCommandLine()", msg);
        }
        if (strncmp(argv[i+1], "--", 2) == 0) {
          std::string msg = "Missing value for parameter [" + std::string(argv[i]) + "]";
          return Status::IOError("ConfigReader::ReadCommandLine()", msg);
        }
        i++;
        value = argv[i];
      }

      Status s = parameters[argument]->Parse(argument, value, "command-line", 0);
      if (!s.IsOK()) return s;
      mandatories_.erase(argument);
      i++;
    }

    return Status::OK();
  }


  Status ParseFile(std::string filepath) {
                         
    std::map<std::string, Parameter*> parameters;
    for (auto& p: parameters_) {
      parameters[p->name] = p;
    }
    
    struct stat info;
    if (stat(filepath.c_str(), &info) != 0) {
      return Status::IOError("ConfigParser", "The file specified does not exists: " + filepath);
    }

    std::ifstream file(filepath);
    std::string str;
    std::regex regex_config {"[\\s]*([^\\s]+)[\\s]+(.*)[\\s]*"};

    int line_number = 0;
    while (std::getline(file, str)) {
      line_number += 1;

      // Deletes the comment if any
      char *line = const_cast<char*>(str.c_str());
      char *ptr = line;
      while (*ptr != '\0' && *ptr != '#') ptr++;
      *ptr = '\0';

      // Checks if this is just an empty
      int size = ptr - line;
      ptr = line;
      while (*ptr != '\0' && (*ptr == ' ' || *ptr == '\t')) ptr++;
      if ((ptr - line) == size) continue;

      std::smatch matches;
      std::string line_cleaned(line);
      if (!std::regex_search(line_cleaned, matches, regex_config) || matches.size() != 3) {
        std::string str_line_number = "Error in file [" + filepath + "] on line " + std::to_string(line_number);
        return Status::IOError("ConfigParser", str_line_number);
      }

      std::string config(matches[1]);
      std::string value(matches[2]);

      if (parameters.find(config) == parameters.end()) {
        if (error_if_unknown_parameters) {
          std::string str_line_number = "Unknown parameter [" + config + "] in file [" + filepath + "] on line " + std::to_string(line_number);
          return Status::IOError("ConfigParser", str_line_number);
        } else {
          continue;
        }
      }

      Status s = parameters[config]->Parse(config, value, filepath, line_number);
      mandatories_.erase(config);
      if (!s.IsOK()) return s;
    }
    return Status::OK();
  }

  bool error_if_unknown_parameters;

 private:
  std::vector<kdb::Parameter*> parameters_;
  std::set<std::string> mandatories_;
};

} // namespace kdb

#endif // KINGDB_CONFIG_PARSER_H_
