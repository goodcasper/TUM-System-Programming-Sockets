#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cstdlib>
#include <unistd.h>  

#include "utils.h"

// 兩個mutex用來保護 cout/cerr，避免多個 thread 同時輸出造成文字混亂
static std::mutex cout_mutex;
static std::mutex cerr_mutex;

// 連線 → 送一堆操作 → 送結束信號 → 等回覆 → 印出結果 → 關閉
void client_worker(int thread_id,
                   const std::string host,
                   int port,
                   int num_messages,
                   int add,
                   int sub) {
                
    // 每個 thread 自己建立到 server 的連線
    int sockfd = connect_socket(host.c_str(), port);
    if (sockfd < 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id << ": failed to connect to "
                  << host << ":" << port << "\n";
        return;
    }

  
    // 每個 client thread 都會對 server 的共享 counter 產生操作。
    for (int i = 0; i < num_messages; ++i) {
        int32_t op_type; // 永遠是4 byte int 有可能依據平台變動而不同
        int64_t arg;

        if (i % 2 == 0) {
            // 偶數 index：做 ADD，增加 add
            op_type = OPERATION_ADD;
            arg = static_cast<int64_t>(add);
        } else {
            // 奇數 index：做 SUB，減少 sub
            op_type = OPERATION_SUB;
            arg = static_cast<int64_t>(sub);
        }

        // 將要求序列化成 protobuf 後送到sever.  server那裡有用read在等
        if (send_msg(sockfd, op_type, arg) != 0) {
            std::lock_guard<std::mutex> lock(cerr_mutex);
            std::cerr << "Thread " << thread_id
                      << ": send_msg failed while sending request " << i << "\n";
            close(sockfd);
            return;
        }
    }

    // 傳完加減操作傳終止訊息 server那裡有用read在等
    // 要求 server 回傳目前的 counter。
    if (send_msg(sockfd, OPERATION_TERMINATION, 0) != 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": send_msg TERMINATION failed\n";
        close(sockfd);
        return;
    }

    
    // 都已經在 server 那邊處理完成。
    int32_t resp_type = 0;
    int64_t counter_value = 0;

    // server 的回覆也同樣是 protobuf 格式，透過 recv_msg 反序列化讀取counter的值
    if (recv_msg(sockfd, &resp_type, &counter_value) != 0) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": recv_msg failed when waiting for COUNTER\n";
        close(sockfd);
        return;
    }

    // server 應該回傳 COUNTER，若不是就表示 protocol 沒對上
    if (resp_type != OPERATION_COUNTER) {
        std::lock_guard<std::mutex> lock(cerr_mutex);
        std::cerr << "Thread " << thread_id
                  << ": unexpected response type " << resp_type << "\n";
        close(sockfd);
        return;
    }

   
    {
        std::lock_guard<std::mutex> lock(cout_mutex); // 建立 lock 這個物件，同時自動鎖住 cout_mutex，當 lock 離開作用域時自動解鎖
        std::cout << counter_value << std::endl;
    }

    // 關閉 socket。每個 thread 自己收自己的連線。
    close(sockfd);
}

int main(int argc, char *argv[]) {

    if (argc < 7) {
        std::cerr << "usage: ./client <num_threads> <hostname> <port> "
                     "<num_messages> <add> <sub>\n";
        return 1;
    }

    // 解析輸入參數
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

    // 建立 num_threads 個 client threads
    // 每個 thread 都會執行 client_worker，彼此之間沒有共享資料（除了 cout/cerr）
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        // 建立thread並讓他執行client_worker
        threads.emplace_back(client_worker,
                             i,
                             host,
                             port,
                             num_messages,
                             add,
                             sub);
    }

  
    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    return 0;
}
