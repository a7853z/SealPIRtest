#include "db_center.h"
#include "common.h"
#include <iostream>
#include <fstream>

using namespace seal;

static const size_t MAX_HIT_SEQ = 1000000000;

DbCenter::DbCenter():total_size(0), hit_seq(0) {
    db_groups = new DbData[number_of_groups];
}

DbCenter::~DbCenter() {
    for (uint32_t i = 0; i < number_of_groups; i++) {
        release_db(i);
    }
    delete []db_groups;
}

// 保证hit_seq不溢出
void DbCenter::ensure_valid_hit_seq(uint32_t id_mod) {
    if (db_groups[id_mod].hit_seq < MAX_HIT_SEQ) return;
    for (uint32_t i = 0; i < number_of_groups; i++) {
        if (i != id_mod) db_groups[i].hit_seq = 0;
    }
    hit_seq = 0;
    db_groups[id_mod].hit_seq = ++hit_seq;
}

// 保证内存不超过最大值
void DbCenter::ensure_valid_memory_size(uint32_t id_mod) {
    uint32_t min_id_mod;
    uint32_t min_hit_seq;
    while (total_size / 1024.0 / 1024.0 / 1024.0 > max_memory_db_size) {
        min_id_mod = -1;
        min_hit_seq = MAX_HIT_SEQ;
        for (uint32_t i = 0; i < number_of_groups; i++) {
            if (id_mod == i) continue;
            if (db_groups[i].db.empty()) continue;
            if (db_groups[i].size == 0) continue;
            if (db_groups[i].hit_seq < min_hit_seq) {
                min_hit_seq = db_groups[i].hit_seq;
                min_id_mod = i;
            }
        }
        if (min_id_mod == -1) break;
        release_db(min_id_mod);
    }
}

void DbCenter::release_db(uint32_t id_mod) {
    Database db = db_groups[id_mod].db;
    db.clear();
    vector(db).swap(db);
    cout << "release db: " << id_mod << endl;
    cout << "before size: " << total_size / 1024.0 / 1024.0 / 1024.0 << endl;
    total_size -= db_groups[id_mod].size;
    cout << "before size: " << total_size / 1024.0 / 1024.0 / 1024.0 << endl;
    db_groups[id_mod].size = 0;
    db_groups[id_mod].hit_seq = 0;
}

unique_ptr<Database> DbCenter::get_db(uint32_t id_mod, SEALContext context, uint32_t db_size) {
    unique_ptr<Database> ret;
    if (id_mod < 0 || id_mod >= number_of_groups) return ret;
    {
        lock_guard<mutex> lock(db_mutex);
        ret.reset(new Database(db_groups[id_mod].db));
        db_groups[id_mod].hit_seq = ++hit_seq;
        ensure_valid_hit_seq(id_mod);
    }
    if ((*ret).empty()) {
        size_t size;
        ret = move(read_db_from_disk(id_mod, context, db_size, &size));
        {
            lock_guard<mutex> lock(db_mutex);
            db_groups[id_mod].db = *ret;
            db_groups[id_mod].size = size;
            db_groups[id_mod].hit_seq = ++hit_seq;
            total_size += size;
            ensure_valid_hit_seq(id_mod);
            ensure_valid_memory_size(id_mod);
        }
    }

    return ret;
}

unique_ptr<Database> DbCenter::read_db_from_disk(uint32_t id_mod, SEALContext context, uint32_t db_size, size_t* ret_size) {


    char db_file[40];
    sprintf(db_file, "processed_dbs/db_%d.bin", id_mod);


    auto result = make_unique<vector<Plaintext>>();
    result->reserve(db_size);

    size_t total = 0;

    ifstream is(db_file, ios::binary | ios::in);
    size_t size1;
    char *buffer = new char[1000000];
    is.read(reinterpret_cast<char*>(&size1), streamsize(sizeof(size_t)));
    cout<<"size1:"<<size1<<endl;
    for (size_t i = 0; i < size1; i++) {
        memset(buffer, 0, 1000000);
        uint32_t size2;
        is.read(reinterpret_cast<char*>(&size2), streamsize(sizeof(uint32_t)));
        total += size2;
        Plaintext pt;
        is.read(buffer, size2);
        string pt_string(buffer, size2);
        stringstream pt_stream;
        pt_stream << pt_string;
        pt.load(context, pt_stream);
        result->push_back(move(pt));
    }
    delete[] buffer;
    is.close();

    if (ret_size) *ret_size = total;

    return result;
}