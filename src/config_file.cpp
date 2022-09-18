#include "config_file.h"
#include <fstream>

string trim(const string& source, const char* delims = " \t\r\n") {
    string result(source);
    string::size_type index = result.find_last_not_of(delims);
    if(index != string::npos)
        result.erase(index + 1);
    index = result.find_first_not_of(delims);
    if(index != string::npos)
        result.erase(0, index);
    else
        result.erase();
    return result;
}

string ConfigFile::config_path = "";

ConfigFile::ConfigFile() {
    ifstream is(config_path.c_str());
    string line;
    int equal_index;
    string key;
    string value;
    while (getline(is, line)) {
        line = trim(line);
        if (!line.length()) continue;
        if (line[0] == '#') continue;
        if (line[0] == ';') continue;
        equal_index = line.find('=');
        key = trim(line.substr(0, equal_index));
        value = trim(line.substr(equal_index + 1));
        if (!key.length()) continue;
        if (!value.length()) continue;
        key2value[key] = value;
    }
}

string ConfigFile::get_value(const string& key) {
    return key2value[key];
}

bool ConfigFile::get_value_bool(const string& key) {
    string value = key2value[key];
    if (value == "true") return true;
    return false;
}

int ConfigFile::get_value_int(const string& key) {
    return stoi(key2value[key]);
}

uint32_t ConfigFile::get_value_uint32(const string& key) {
    return static_cast<uint32_t>(stoul(key2value[key]));
}

uint64_t ConfigFile::get_value_uint64(const string& key) {
    return static_cast<uint64_t>(stoull(key2value[key]));
}

float ConfigFile::get_value_float(const string& key) {
    return stof(key2value[key]);
}

bool ConfigFile::key_exist(const string& key) {
    if(key2value.count(key)==0) {
        return false;
    }
    return true;
}