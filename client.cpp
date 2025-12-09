#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cstdlib>
#include <unistd.h>  // close()

#include "utils.h"

static std::mutex cout_mutex;
static std::mutex cerr_mutex;

void client_worker(int thread_id,
                   const std::string host,
                   int port,
                   int num_messages,
                   int add,
                   int sub) {
    // 1. 每個 thread 各自連線到 server
    int sockfd = connect_socket(host.c_str(), port);
    if (sockfd < 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id << ": failed to connect to "
                  << host << ":" << port << "\n";
        return;
    }

    // 2. 傳送 num_messages 個訊息，ADD / SUB 交替，從 ADD 開始
    for (int i = 0; i < num_messages; ++i) {
        int32_t op_type;
        int64_t arg;

        if (i % 2 == 0) {
            // 偶數 index：ADD
            op_type = OPERATION_ADD;
            arg = static_cast<int64_t>(add);
        } else {
            // 奇數 index：SUB
            op_type = OPERATION_SUB;
            arg = static_cast<int64_t>(sub);
        }

        if (send_msg(sockfd, op_type, arg) != 0) {
            std::lock_guard<std::mutex> lock(cerr_mutex);
            std::cerr << "Thread " << thread_id
                      << ": send_msg failed while sending request " << i << "\n";
            close(sockfd);
            return;
        }
    }

    // 3. 傳送 TERMINATION 訊息
    if (send_msg(sockfd, OPERATION_TERMINATION, 0) != 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": send_msg TERMINATION failed\n";
        close(sockfd);
        return;
    }

    // 4. 接收 COUNTER 回覆
    int32_t resp_type = 0;
    int64_t counter_value = 0;

    if (recv_msg(sockfd, &resp_type, &counter_value) != 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": recv_msg failed when waiting for COUNTER\n";
        close(sockfd);
        return;
    }

    if (resp_type != OPERATION_COUNTER) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": unexpected response type " << resp_type << "\n";
        close(sockfd);
        return;
    }

    // 5. 印出 server 傳回來的 counter 值（作業說每個 client thread 要印）
    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << counter_value << std::endl;  // 只印數字 + 換行，避免影響測試
    }

    // 6. 關閉 socket
    close(sockfd);
}

int main(int argc, char *argv[]) {
    if (argc < 7) {
        std::cerr << "usage: ./client <num_threads> <hostname> <port> "
                     "<num_messages> <add> <sub>\n";
        return 1;
    }

    int num_threads  = std::atoi(argv[1]);
    std::string host = argv[2];
    int port         = std::atoi(argv[3]);
    int num_messages = std::atoi(argv[4]);
    int add          = std::atoi(argv[5]);
    int sub          = std::atoi(argv[6]);

    if (num_threads <= 0 || num_messages < 0) {
        std::cerr << "invalid arguments\n";
        return 1;
    }

    // 建立 num_threads 個 thread，每個 thread 獨立連線並送訊息
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(client_worker,
                             i,
                             host,
                             port,
                             num_messages,
                             add,
                             sub);
    }

    // 等待所有 threads 結束
    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return 0;
}
