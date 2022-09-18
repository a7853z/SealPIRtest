#ifndef DB_CENTER_H
#define DB_CENTER_H

#include <vector>
#include <cstdint>
#include <mutex>
#include "seal/seal.h"
#include "pir.hpp"
using namespace std;
using namespace seal;
using namespace seal::util;


class DbData {
    public:
        DbData():size(0), hit_seq(0){}
        Database db;
        size_t size;
        size_t hit_seq; // 被获取的序列号
};

// split_db的缓存
class DbCenter {
    public:
        // 获取单例
        static DbCenter& get_instance() {
            static DbCenter db_center;
            return db_center;
        }
        // 获取db缓存或从磁盘中读取
        unique_ptr<Database> get_db(uint32_t id_mod, SEALContext context, uint32_t db_size);
        // 释放db缓存
        void release_db(uint32_t id_mod);
        // 从磁盘加载split_db；读取的split_db大小保存到ret_size中
        static unique_ptr<Database> read_db_from_disk(uint32_t id_mod, SEALContext context, uint32_t db_size, size_t* ret_size = nullptr);

    private:
        DbCenter();
        ~DbCenter();
        void ensure_valid_hit_seq(uint32_t id_mod);
        void ensure_valid_memory_size(uint32_t id_mod);
        DbData* db_groups;
        mutex db_mutex;
        size_t total_size;
        size_t hit_seq;
};

#endif //DB_CENTER_H