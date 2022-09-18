//
// Created by AAGZ0452 on 2022/8/11.
//

#ifndef ONIONPIR_NETSERVER_H
#define ONIONPIR_NETSERVER_H

#endif //ONIONPIR_NETSERVER_H

#include <errno.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <fstream>
using namespace std;

#define BUFFER_SIZE 5000000 // TODO 很多线程的话，内存占用大？

// 用来存储某个连接相关的数据
class ConnData {
public:
    int connect_fd = -1;
    char buffer[BUFFER_SIZE];  //40w字节buffer用来与client通讯
    bool send_ready = true;
    int last_id_mod = -1;  //用来判断db是否已处理  used to determine if the db is preprocessed
    uint32_t number_of_items = 0;  //百万不可区分度， 具体需要从服务器获取
};

class NetServer {
public:
    int listen_fd = -1;
    uint32_t server_port;
    string server_ip;
    sockaddr_in server_addr;
    uint32_t sin_size, data_len;
    int init_net_server();  //initialize net server and listen
    int wait_connection();
    static int one_time_receive(ConnData* conn_data, string &message);
    static bool one_time_send(ConnData* conn_data, const char * buf, uint32_t size);
    NetServer(string ip ="127.0.0.1" , int port = 11111) {
        server_ip = ip;
        server_port = port;
    }
    ~NetServer(){
        close(listen_fd);
    }
};