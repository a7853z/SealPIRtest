//
// Created by AAGZ0452 on 2022/8/11.
//

#ifndef ONIONPIR_NETCLIENT_H
#define ONIONPIR_NETCLIENT_H

#endif //ONIONPIR_NETCLIENT_H
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

#define BUFFER_SIZE 500000

class NetClient {
public:
    int connect_fd=-1;
    char buffer[BUFFER_SIZE];
    struct sockaddr_in server_addr;    // 服务器地址结构
    uint32_t server_port;
    string server_ip;
    void init_client();
    int one_time_receive(string &message);
    bool one_time_send(const char * buf, uint32_t size);
    bool send_ready = true;
    NetClient(string ip ="127.0.0.1" , int port = 11111) {
        server_ip = ip;
        server_port = port;
    }
    ~NetClient(){
        close(connect_fd);
    }
};