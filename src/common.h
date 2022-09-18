//
// Created by AAGZ0452 on 2022/8/26.
//

#ifndef ONIONPIR_COMMON_H
#define ONIONPIR_COMMON_H

#include "SHA256.h"

#include <string>
#include <unistd.h> // linux only
#include <sys/stat.h> // linux only
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

#endif //ONIONPIR_COMMON_H