#include "utils.h"
#include <arpa/inet.h>   
#include <netdb.h>      
#include <sys/socket.h> 
#include <sys/types.h>
#include <unistd.h>      
#include <cstring>
#include <string>

#include "message.pb.h"



extern "C" { // 讓 C / C++ 可以互相呼叫

// server 端用來建立一個 TCP listening socket
int listening_socket(int port) { 
    int sockfd = socket(AF_INET, SOCK_STREAM, 0); // 建立一個IPv4 TCP socket 
    if (sockfd < 0) { // 檢查是否建立成功
        perror("socket");
        return -1;
    }

    int yes = 1;
    // 如果 server 重啟，允許立刻重新使用同一個 port
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt");
        
    }

    struct sockaddr_in addr; // 該結構紀錄了ip+port
    std::memset(&addr, 0, sizeof(addr)); // 把該結構的記憶體都清成0 做初始化
    addr.sin_family = AF_INET; // IPv4
    // htonl 從 host byte order 轉為 network byte order (big-endian)
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 設定為127.0.0.1 代表自己 (只能接受本機端（localhost）連到這個 port)
    addr.sin_port = htons(static_cast<uint16_t>(port)); // 設定port 轉換型別

    // 把 sockfd 這個 socket 綁在 addr 指定的 IP + port 上
    // 我使用的結構是sockaddr_in 要使用reinterpret_cast強制轉型
    if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd);
        return -1;
    }

    // 把 socket 變成監聽模式 (等待連線的狀態) SOMAXCONN(macro 通常是128) 為 OS 允許的最大排隊等待連線數
    if (listen(sockfd, SOMAXCONN) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

// 根據主機名稱和port 找到一個可連線的位址並連上
int connect_socket(const char *hostname, const int port) {
    struct addrinfo hints; // 用來告訴 getaddrinfo() 我們希望取得什麼種類的位址格式
    struct addrinfo *res = nullptr; // getaddrinfo() 執行後，會把查詢結果的 linked list 放在這個指標裡

    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;          // IPv4
    hints.ai_socktype = SOCK_STREAM;    // TCP

    std::string port_str = std::to_string(port); // getaddrinfo() 要求 port 必須是字串
    // 把 hostname 字串 轉成 OS 能用的 socket 位址結構

    // res會存多個可嘗試連線的位址組成的 linked list
    int ret = getaddrinfo(hostname, port_str.c_str(), &hints, &res); // 第二個參數要求是C語言字串
    if (ret != 0)
        return -1;

    int sockfd = -1;
    // 將可連線的位址列表逐一嘗試 只要一個連線成功就跳出
    for (struct addrinfo *p = res; p != nullptr; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd < 0)
            continue;

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == 0) {
            break;  // success
        }

        close(sockfd);
        sockfd = -1;
    }
    // 在getaddrinfo時會從heap動態分配記憶體空間 所以用完要free掉
    freeaddrinfo(res);

    // 可以被read/write的都是檔案描述符 依據開啟的順序分配號碼
    return sockfd;
}

int accept_connection(int sockfd) {
    // 等待一個新的 client 連線，回傳對應的 fd (溝通通道)
    int client_fd = accept(sockfd, nullptr, nullptr);
    if (client_fd < 0)
        return -1;
    return client_fd;
}


//  Helper: full read（確保讀到指定長度）
static bool read_full(int fd, void *buf, size_t len) {
    uint8_t *ptr = reinterpret_cast<uint8_t*>(buf); // // 轉成 uint8_t*（1 byte 為單位的指標 這樣才能做 ptr + total 來移動位置
    size_t total = 0;
    while (total < len) {
        // 一次 read 可能讀不滿，因此要累積直到 len
        // 如果沒資料thread會等到有資料寫來
        ssize_t n = read(fd, ptr + total, len - total); // fd讀資料 從ptr+total開始 讀len-total個byte
        if (n <= 0)
            return false;   // 對方關閉或讀取錯誤
        total += n;
    }
    return true;
}


//  Helper: full write（確保寫完指定長度）
static bool write_full(int fd, const void *buf, size_t len) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(buf); 
    size_t total = 0;
    while (total < len) {
        // write 同樣可能一次只寫部分資料
        ssize_t n = write(fd, ptr + total, len - total);
        if (n <= 0)
            return false;
        total += n;
    }
    return true;
}



// 從 socket 收一個 protobuf 訊息，並解析出 operation 與 argument
// protobuf 前4 bytes用來表達訊息長度
int recv_msg(int sockfd, int32_t *operation_type, int64_t *argument) {
    if (!operation_type || !argument)
        return 1;

    uint32_t size_net = 0;
    // 先讀取訊息長度 從sockfd讀sizeof(size_net)長度的資料到 &size_net
    if (!read_full(sockfd, &size_net, sizeof(size_net)))
        return 1;

    uint32_t size = ntohl(size_net); // network order -> host order）
    if (size == 0)
        return 1;

    // 根據 size 開 buffer 接收 payload
    std::string buffer(size, '\0');
    if (!read_full(sockfd, buffer.data(), size)) // .data()功能是 string 的內容轉成 char*
        return 1;

    // 反序列化成 protobuf message
    // string 裡就是一串 bytes 要把它解析成結構化資料
    sockets::message msg;
    if (!msg.ParseFromString(buffer))
        return 1;

    *argument = 0;

    // 依照 message type 設定對應的操作與參數
    sockets::message::OperationType type = msg.type();
    switch (type) {
        case sockets::message::ADD:
            *operation_type = OPERATION_ADD;
            *argument = msg.argument();
            break;
        case sockets::message::SUB:
            *operation_type = OPERATION_SUB;
            *argument = msg.argument();
            break;
        case sockets::message::TERMINATION:
            *operation_type = OPERATION_TERMINATION;
            break;
        case sockets::message::COUNTER:
            *operation_type = OPERATION_COUNTER;
            *argument = msg.argument();
            break;
        default:
            return 1; // 未知的操作類型
    }

    return 0;
}



//  將operation / argument 打包成 protobuf 並送出
int send_msg(int sockfd, int32_t operation_type, int64_t argument) {
    sockets::message msg;

    // 設定 protobuf 欄位
    switch (operation_type) {
        case OPERATION_ADD:
            msg.set_type(sockets::message::ADD);
            msg.set_argument(argument);
            break;
        case OPERATION_SUB:
            msg.set_type(sockets::message::SUB);
            msg.set_argument(argument);
            break;
        case OPERATION_TERMINATION:
            msg.set_type(sockets::message::TERMINATION);
            break;
        case OPERATION_COUNTER:
            msg.set_type(sockets::message::COUNTER);
            msg.set_argument(argument);
            break;
        default:
            return 1;
    }

    // 序列化成字串 payload
    std::string payload;
    // 把資料結構(msg)壓成一串 bytes 存進 payload 字串裡
    if (!msg.SerializeToString(&payload))
        return 1;

    uint32_t size = payload.size();
    // host order -> network order
    uint32_t size_net = htonl(size);

    // 先送 size，再送內容
    if (!write_full(sockfd, &size_net, sizeof(size_net)))
        return 1;

    if (!write_full(sockfd, payload.data(), size))
        return 1;

    return 0;
}

}