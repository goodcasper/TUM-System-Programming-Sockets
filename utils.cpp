#include "utils.h"

#include <arpa/inet.h>   // inet_pton, htons, htonl
#include <netdb.h>       // getaddrinfo, freeaddrinfo
#include <sys/socket.h>  // socket, connect, bind, listen, accept
#include <sys/types.h>
#include <unistd.h>      // close, read, write
#include <cstring>
#include <string>

#include "message.pb.h"



// ---------------------------------------------------------------
//  listening_socket(port)
// ---------------------------------------------------------------
extern "C" {

int listening_socket(int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    int yes = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt");
        // 即使這裡失敗，一般也可以繼續嘗試 bind，所以不直接 return
    }

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // 127.0.0.1
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(sockfd);
        return -1;
    }

    if (listen(sockfd, SOMAXCONN) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }

    return sockfd;
}


// ---------------------------------------------------------------
//  connect_socket(hostname, port)
// ---------------------------------------------------------------
int connect_socket(const char *hostname, const int port) {
    struct addrinfo hints;
    struct addrinfo *res = nullptr;

    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;          // IPv4
    hints.ai_socktype = SOCK_STREAM;    // TCP

    std::string port_str = std::to_string(port);
    int ret = getaddrinfo(hostname, port_str.c_str(), &hints, &res);
    if (ret != 0)
        return -1;

    int sockfd = -1;
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

    freeaddrinfo(res);
    return sockfd;
}

// ---------------------------------------------------------------
//  accept_connection(listen_fd)
// ---------------------------------------------------------------
int accept_connection(int sockfd) {
    int client_fd = accept(sockfd, nullptr, nullptr);
    if (client_fd < 0)
        return -1;
    return client_fd;
}

// ---------------------------------------------------------------
//  Helper: full read
// ---------------------------------------------------------------
static bool read_full(int fd, void *buf, size_t len) {
    uint8_t *ptr = reinterpret_cast<uint8_t*>(buf);
    size_t total = 0;
    while (total < len) {
        ssize_t n = read(fd, ptr + total, len - total);
        if (n <= 0)
            return false;
        total += n;
    }
    return true;
}

// ---------------------------------------------------------------
//  Helper: full write
// ---------------------------------------------------------------
static bool write_full(int fd, const void *buf, size_t len) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(buf);
    size_t total = 0;
    while (total < len) {
        ssize_t n = write(fd, ptr + total, len - total);
        if (n <= 0)
            return false;
        total += n;
    }
    return true;
}

// ---------------------------------------------------------------
//  recv_msg(fd, *operation, *argument)
// ---------------------------------------------------------------
int recv_msg(int sockfd, int32_t *operation_type, int64_t *argument) {
    if (!operation_type || !argument)
        return 1;

    uint32_t size_net = 0;
    if (!read_full(sockfd, &size_net, sizeof(size_net)))
        return 1;

    uint32_t size = ntohl(size_net);
    if (size == 0)
        return 1;

    std::string buffer(size, '\0');
    if (!read_full(sockfd, buffer.data(), size))
        return 1;

    sockets::message msg;
    if (!msg.ParseFromString(buffer))
        return 1;

    *argument = 0;

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
            return 1;
    }

    return 0;
}

// ---------------------------------------------------------------
//  send_msg(fd, operation, argument)
// ---------------------------------------------------------------
int send_msg(int sockfd, int32_t operation_type, int64_t argument) {
    sockets::message msg;

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

    std::string payload;
    if (!msg.SerializeToString(&payload))
        return 1;

    uint32_t size = payload.size();
    uint32_t size_net = htonl(size);

    if (!write_full(sockfd, &size_net, sizeof(size_net)))
        return 1;

    if (!write_full(sockfd, payload.data(), size))
        return 1;

    return 0;
}

} // extern "C"
