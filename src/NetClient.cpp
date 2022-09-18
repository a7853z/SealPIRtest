//
// Created by AAGZ0452 on 2022/8/11.
//

#include "NetClient.h"

void NetClient::init_client() {
    cout<<"server ip:"<<server_ip<<" server port:"<<server_port<<endl;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr(server_ip.c_str());
    server_addr.sin_port = htons(server_port);

    connect_fd = socket(AF_INET,SOCK_STREAM,0);
    if(connect_fd<0){
        cout<<"socket_error"<<endl;
    }

    int res = connect(connect_fd, (struct sockaddr*)(&server_addr), sizeof(server_addr));
    if (res == -1)
    {
        cout<<"connect error"<<endl;
    }
    cout<<"connect_fd:"<<connect_fd<<endl;
    cout<<"Connect to server success"<<endl;
}

int NetClient::one_time_receive(string &message){
    //清空buffer
    memset(&buffer, 0, sizeof(buffer));
    if(connect_fd < 0) {
        cout<<"wrong connect_fd:"<<connect_fd<<endl;
        return -1;
    }
    //receive length first
    uint32_t recv_bytes;
    uint32_t size;
    recv_bytes = recv(connect_fd, buffer, sizeof(buffer), 0);
    memcpy(&size, buffer, sizeof(size));
    //cout<<"net_client:: received bytes:"<<recv_bytes<<" packet length:"<<size<<endl;

    size-=(recv_bytes-sizeof(size));

    //when recv_butes==BUFFER_SIZE extra char
    string temp(buffer+4, recv_bytes-sizeof(size));
    message+=temp;
    //cout<<"net_server: message length:"<<message.length()<<endl;
    //buffer[recv_bytes]='\0';


    while(size>0){
        memset(&buffer, 0, sizeof(buffer));
        recv_bytes = recv(connect_fd, buffer, sizeof(buffer), 0);
        //buffer[recv_bytes]='\0';
        //cout<<"received bytes:"<<recv_bytes<<endl;

        //when recv_bytes==BUFFER_SIZE 导致 extra char
        string temp1(buffer, recv_bytes);
        message += temp1;
        //cout<<"message length:"<<message.length()<<endl;
        size-=recv_bytes;
    }

    cout<<"net_client:: received message length:"<<message.length()<<endl;
    //send 'finished'
    cout<<"received, sending finished message"<<endl;
    char *msg = "finished";
    send(connect_fd, msg, strlen(msg), 0);
    return size;
}

bool NetClient::one_time_send(const char * buf, uint32_t size){
    //连续发送  直到发送完
    //cout<<"send ready:"<<send_ready<<endl;
    if(!send_ready) {
        cout<<"net_client:: not ready to send, wait till finish"<<endl;
        uint32_t recv_bytes = recv(connect_fd, (char*)&buffer, 8, 0);  //"finished" length 8
        //buffer[recv_bytes] = '\0';
        if(strcmp(buffer, "finished")==0) {
            cout<<"net_client:: finished sending a message"<<endl;
            send_ready = true;
        }
        else {
            cout<<"net_client:: didn't receive the finish message, received buffer:"<<buffer<<endl;
        }
    }
    while(!send_ready) {
    }

    //send length first
    uint32_t length = size;
    cout<<"net_client:: sending message length:"<<length<<endl;
    send(connect_fd, (char *)&length, sizeof(length), 0);

    uint32_t sended = 0;
    while (size>0)
    {
        int SendSize= send(connect_fd, buf+sended, size, 0);
        sended +=SendSize;
        //cout<<"net_client:: Sended size:"<<SendSize<<endl;
        if(SendSize<0) {
            cout<<"net_client:: send error"<<SendSize<<endl;
            return false;
        }
        size = size - SendSize;//用于循环发送且退出功能
        //buf += SendSize;//用于计算已发buf的偏移量
    }

    send_ready = false;

    cout<<"net_client:: waiting for finish reply from net_server."<<endl;
    memset(&buffer, 0, sizeof(buffer));
    if(connect_fd < 0) {
        cout<<"net_client:: wrong connect_fd:"<<connect_fd<<endl;
        return -1;
    }
    uint32_t recv_bytes = recv(connect_fd, (char*)&buffer, 8, 0);  //"finished" length 8
    //buffer[recv_bytes] = '\0';

    if(strcmp(buffer, "finished")==0) {
        cout<<"net_client:: finished sending a message"<<endl;
        send_ready = true;
    }
    else {
        cout<<"net_client:: didn't receive the finish message, received buffer:"<<buffer<<endl;
    }
    return true;
}
