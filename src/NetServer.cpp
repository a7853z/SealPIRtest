//
// Created by AAGZ0452 on 2022/8/11.
//
#include "NetServer.h"
using namespace std;

int NetServer::init_net_server() {
    //初始化
    cout<<"net_server::server ip:"<<server_ip<<" server port:"<<server_port<<endl;
    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    listen_fd = socket(AF_INET,SOCK_STREAM,0);
    int optval = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
    int ret;
    ret = bind(listen_fd,(struct sockaddr*)&server_addr,sizeof(server_addr));
    if(ret == -1) {
        cout<<"net_server:: bind error"<<endl;
        return -1;
    }else {
        cout<<"SERVER: bind success"<<endl;
    }

    ret = listen(listen_fd, 128);
    if(ret == -1) {
        cout<<"net_server:: listen error"<<endl;
        return -1;
    }
    cout<<"net_server:: listen success, listen fd:"<<listen_fd<<endl;
    char client_ip[20];
    return 0;
}

int NetServer::wait_connection() {
    sockaddr_in client_addr;
    uint32_t client_addr_len = sizeof(client_addr);
    int connect_fd = accept(listen_fd, (sockaddr*)(&client_addr), &client_addr_len);
    if(connect_fd<0){
        cout<<"net_server::accept error"<<endl;
    }

    cout<<"net_server::connect_fd:"<<connect_fd<<endl;
    //输出客户端信息
    cout << "net_server::client ip:" << inet_ntoa(client_addr.sin_addr) <<endl;
    cout << "net_server::client port:" <<  ntohs(client_addr.sin_port) <<endl;
    return connect_fd;
}

int NetServer::one_time_receive(ConnData* conn_data, string &message){
    //清空buffer
    memset(&(conn_data->buffer), 0, sizeof(conn_data->buffer));
    if(conn_data->connect_fd < 0) {
        cout<<"net_server::wrong connect_fd:"<<conn_data->connect_fd<<endl;
        return -1;
    }
    //receive length first
    uint32_t recv_bytes;
    uint32_t size;
    recv_bytes = recv(conn_data->connect_fd, conn_data->buffer, sizeof(conn_data->buffer), 0);
    if (recv_bytes == 0) {
        cout<<"receive 0 bytes!"<<endl;
        return -1;
    }
    memcpy(&size, conn_data->buffer, sizeof(size));
    //cout<<"net_server: received bytes:"<<recv_bytes<<" packet length:"<<size<<endl;

    size-=(recv_bytes-sizeof(size));

    //when recv_butes==BUFFER_SIZE extra char
    string temp(conn_data->buffer+4, recv_bytes-sizeof(size));
    message+=temp;
    //cout<<"net_server: message length:"<<message.length()<<endl;
    //buffer[recv_bytes]='\0';


    while(size>0){
        memset(&(conn_data->buffer), 0, sizeof(conn_data->buffer));
        recv_bytes = recv(conn_data->connect_fd, conn_data->buffer, sizeof(conn_data->buffer), 0);
        //buffer[recv_bytes]='\0';
        //cout<<"net_server::received bytes:"<<recv_bytes<<endl;

        //when recv_bytes==BUFFER_SIZE 导致 extra char
        string temp1(conn_data->buffer, recv_bytes);
        message += temp1;
        //cout<<"message length:"<<message.length()<<endl;
        size-=recv_bytes;
    }

    cout<<"net_server::received message length:"<<message.length()<<endl;
    //send 'finished'
    cout<<"net_server::received, sending finished message"<<endl;
    char *msg = "finished";
    send(conn_data->connect_fd, msg, strlen(msg), 0);
    return size;
}

bool NetServer::one_time_send(ConnData* conn_data, const char * buf, uint32_t size){
    //连续发送  直到发送完
    if(!conn_data->send_ready) {
        cout<<"net_server:: not ready to send, wait till finish"<<endl;
        uint32_t recv_bytes = recv(conn_data->connect_fd, (char*)&(conn_data->buffer), 8, 0);  //"finished" length 8
        //buffer[recv_bytes] = '\0';
        if(strcmp(conn_data->buffer, "finished")==0) {
            cout<<"net_server:: finished sending a message"<<endl;
            conn_data->send_ready = true;
        }
        else {
            cout<<"net_server:: didn't receive the finish message, received buffer:"<<conn_data->buffer<<endl;
        }
    }
    while(!conn_data->send_ready) {
    }

    //send length first
    uint32_t length = size;
    cout<<"net_server::sending message length:"<<length<<endl;
    send(conn_data->connect_fd, (char *)&length, sizeof(length), 0);

    uint32_t sended = 0;
    while (size>0)
    {
        int SendSize= send(conn_data->connect_fd, buf+sended, size, 0);
        sended +=SendSize;
        //cout<<"net_server::Sended size:"<<SendSize<<endl;
        if(SendSize<0) {
            cout<<"net_server::send error"<<SendSize<<endl;
            return false;
        }
        size = size - SendSize;//用于循环发送且退出功能
        //buf += SendSize;//用于计算已发buf的偏移量
    }

    conn_data->send_ready = false;

    cout<<"net_server::waiting for finish reply"<<endl;
    memset(&(conn_data->buffer), 0, sizeof(conn_data->buffer));
    if(conn_data->connect_fd < 0) {
        cout<<"net_server::wrong connect_fd:"<<conn_data->connect_fd<<endl;
        return -1;
    }
    uint32_t recv_bytes = recv(conn_data->connect_fd, (char*)&(conn_data->buffer), 8, 0);  //"finished" length 8
    //buffer[recv_bytes] = '\0';

    if(strcmp(conn_data->buffer, "finished")==0) {
        cout<<"net_server::finished sending a message"<<endl;
        conn_data->send_ready = true;
    }
    else {
        cout<<"net_server::didn't receive the finish message, received buffer:"<<conn_data->buffer<<endl;
    }
    return true;
}



