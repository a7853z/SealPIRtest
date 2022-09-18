#include "common.h"

uint32_t N=4096;
uint32_t logt=20;
uint64_t size_per_item=23;
bool use_symmetric = true; // use symmetric encryption instead of public key
// (recommended for smaller query)
bool use_batching = true;  // pack as many elements as possible into a BFV
// plaintext (recommended)
bool use_recursive_mod_switching = true;

uint32_t number_of_groups=90;
string ip = "127.0.0.1";
int port = 11111;
bool process_data=false;
bool process_db=false;
bool use_memory_db=false;
float max_memory_db_size=16.0; // 单位为GB
string batch_id_file = "query_id.csv";
string id_file = "query_data.csv";
bool process_id = false;
string data_file = "query_data.csv";
uint32_t batch_id_number = 1100000;
bool batch_id_preprocess = true;
bool sync_ids = false;

