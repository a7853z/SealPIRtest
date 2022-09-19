//
// Created by AAGZ0452 on 2022/8/26.
//

#ifndef ONIONPIR_COMMON_H
#define ONIONPIR_COMMON_H

#include "SHA256.h"

#include <string>
#include <unistd.h> // linux only
#include <sys/stat.h> // linux only
#include <vector>
#include <cstring>
using namespace std;

extern uint32_t N;
extern uint32_t logt;
extern uint64_t size_per_item;
extern bool use_symmetric;
extern bool use_batching;
extern bool use_recursive_mod_switching;
extern uint32_t number_of_groups;
extern string ip;
extern int port;
extern bool process_data;
extern bool process_db;
extern bool use_memory_db;
extern float max_memory_db_size;
extern string batch_id_file;
extern string id_file;
extern bool process_id;
extern string data_file;
extern uint32_t batch_id_number;
extern bool batch_id_preprocess;
extern bool sync_ids;
extern bool enable_binary_encoding;

inline uint32_t get_id_mod(string query_id, uint32_t number_of_groups)
{
    SHA256 sha;
    sha.update(query_id);
    uint8_t * digest = sha.digest();
    uint32_t id_mod = SHA256::mod(digest, number_of_groups);
    //cout << SHA256::toString(digest) << " mod:"<<id_mod<<std::endl;
    delete[] digest;
    return id_mod;
}

// 文件或目录是否存在
inline bool path_exists(const char *path)
{
    return access(path, F_OK) == 0;
}

// 如果目录没创建，创建它
inline void ensure_dir(const char *path)
{
    if (access(path, F_OK) == -1) {
        mkdir(path, 0755);
    }
}

// 使用字符分割
inline void Stringsplit(const string& str, const char split, vector<string>& res)
{
    if (str == "")        return;
    //在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + split;
    size_t pos = strs.find(split);

    // 若找不到内容则字符串搜索函数返回 npos
    while (pos != strs.npos)
    {
        string temp = strs.substr(0, pos);
        res.push_back(temp);
        //去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(split);
    }
}

inline uint32_t date_to_int(string date){
    vector<string> res;
    Stringsplit(date,'-', res);
    uint32_t year = atoi(res[0].c_str());
    uint32_t month = atoi(res[1].c_str());
    uint32_t day = atoi(res[2].c_str());
    uint32_t date_int = year*12*31 + (month-1)*31+day-1;
    return date_int;
}

inline string int_to_date(uint32_t date_int) {
    uint32_t year = date_int/(12*31);
    uint32_t month = (date_int%(12*31))/31+1;
    uint32_t day = date_int%31+1;
    char date[15];
    if(day<10 and month<10) sprintf(date,"%d-0%d-0%d", year, month, day);
    else if(day<10) sprintf(date,"%d-%d-0%d", year, month, day);
    else if(month<10) sprintf(date,"%d-0%d-%d", year, month, day);
    else sprintf(date,"%d-%d-%d", year, month, day);
    string str_date(date);
    return str_date;
}

inline void binary_encode(string data, char * &data_buffer) {
    //这里需要优化，改成二进制编码
    string date;
    uint32_t date_int;
    char age;
    vector<string> res;
    Stringsplit(data,',', res);
    date = res[0];
    date_int = date_to_int(date);
    age = '\0'+atoi(res[1].c_str());
    double amount = atof(res[2].c_str());
    memcpy(data_buffer, (char *)(&date_int), sizeof(date_int));
    memcpy(data_buffer+sizeof(date_int), &age, sizeof(age));
    memcpy((data_buffer+sizeof(date_int)+sizeof(age)), (char*)&amount, sizeof(amount));  //转成float有精度损失
    //memcpy((data_buffer+sizeof(date_int)+sizeof(age)), res[2].c_str(), res[2].length());
}

inline void binary_decode(const char * data_buffer, string &date, char &age, double &amount) {
    //这里需要优化，改成二进制编码
    uint32_t date_int;
    memcpy(&date_int, data_buffer, sizeof(date_int));
    memcpy(&age, data_buffer+4, sizeof(age));
    memcpy(&amount, data_buffer+5, sizeof(amount));
    date = int_to_date(date_int);
}
#endif //ONIONPIR_COMMON_H