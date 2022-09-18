#ifndef CONFIG_FILE_H
#define CONFIG_FILE_H

#include <string>
#include <map>
using namespace std;

class ConfigFile {
    public:
        // 先设计配置路径，只需设置一次
        static void set_path(const string& path) {
            config_path = path;
        }
        // 再获取配置对象实例
        static ConfigFile& get_instance() {
            static ConfigFile config_file;
            return config_file;
        }
        string get_value(const string& key);
        bool get_value_bool(const string &key);
        int get_value_int(const string& key);
        uint32_t get_value_uint32(const string& key);
        uint64_t get_value_uint64(const string& key);
        float get_value_float(const string& key);
        bool key_exist(const string& key);
    private:
        ConfigFile();

        map<string, string> key2value;
        static string config_path;
};

#endif //CONFIG_FILE_H