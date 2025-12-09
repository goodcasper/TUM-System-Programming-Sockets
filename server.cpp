#include <atomic>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <cstdlib>
#include <unistd.h>      // close, pipe, read, write
#include <sys/select.h>  // select
#include <errno.h>
#include <algorithm>

#include "utils.h"

// 全域 counter（需求：server 管理一個 global counter，原子更新）
std::atomic<int64_t> number{0};

// 印到 stdout 時要加鎖，避免多個 thread 把數字黏在同一行
static std::mutex print_mutex;

// 每個 worker thread 的狀態
struct Worker {
    std::thread thread;           // 該 worker 的 thread
    int wakeup_read_fd = -1;      // pipe 的讀端，用來喚醒 select
    int wakeup_write_fd = -1;     // pipe 的寫端，main thread 寫入喚醒

    std::mutex sockets_mutex;     // 保護 sockets
    std::vector<int> sockets;     // 目前由這個 worker 管理的 client fd

    std::mutex new_conn_mutex;             // 保護 new_connections
    std::queue<int> new_connections;       // main thread newly assigned fds
};

// worker thread 主迴圈：使用 select() 同時處理多個連線
void worker_loop(Worker *worker) {
    while (true) {
        // 1) 把 main thread 指派的新連線搬進 sockets
        {
            std::lock_guard<std::mutex> lock(worker->new_conn_mutex);
            while (!worker->new_connections.empty()) {
                int fd = worker->new_connections.front();
                worker->new_connections.pop();
                std::lock_guard<std::mutex> lock2(worker->sockets_mutex);
                worker->sockets.push_back(fd);
            }
        }

        // 2) 準備 select 的 fd_set
        fd_set readfds;
        FD_ZERO(&readfds);

        int maxfd = worker->wakeup_read_fd;
        FD_SET(worker->wakeup_read_fd, &readfds);

        std::vector<int> snapshot;
        {
            std::lock_guard<std::mutex> lock(worker->sockets_mutex);
            snapshot = worker->sockets;  // 做一份副本，避免 select 期間持有鎖
        }

        for (int fd : snapshot) {
            FD_SET(fd, &readfds);
            if (fd > maxfd) {
                maxfd = fd;
            }
        }

        int ret = select(maxfd + 1, &readfds, nullptr, nullptr, nullptr);
        if (ret < 0) {
            if (errno == EINTR) {
                continue; // 被 signal 打斷就重來
            }
            // 其他錯誤：繼續 looping（實務上可加錯誤處理，但作業不強制）
            continue;
        }

        // 3) 若 wakeup pipe 有資料，讀掉以清除喚醒信號
        if (FD_ISSET(worker->wakeup_read_fd, &readfds)) {
            char buf[64];
            // 把 pipe 中累積的喚醒字節讀光
            while (true) {
                ssize_t n = read(worker->wakeup_read_fd, buf, sizeof(buf));
                if (n <= 0) {
                    break;
                }
                if (n < static_cast<ssize_t>(sizeof(buf))) {
                    break;
                }
            }
        }

        // 4) 處理每個有資料可讀的 client socket
        std::vector<int> to_remove;  // 結束的連線要從 sockets 列表移除

        for (int fd : snapshot) {
            if (!FD_ISSET(fd, &readfds)) {
                continue;
            }

            int32_t op_type = 0;
            int64_t arg = 0;
            // 使用 utils.cpp 的 recv_msg() 讀一個完整訊息
            if (recv_msg(fd, &op_type, &arg) != 0) {
                // client 關閉或錯誤：關閉連線並移除
                close(fd);
                to_remove.push_back(fd);
                continue;
            }

            if (op_type == OPERATION_ADD) {
                number.fetch_add(arg, std::memory_order_relaxed);
            } else if (op_type == OPERATION_SUB) {
                number.fetch_sub(arg, std::memory_order_relaxed);
            } else if (op_type == OPERATION_TERMINATION) {
                // 取得目前 counter
                int64_t current = number.load(std::memory_order_relaxed);

                // 回傳 COUNTER 給 client
                if (send_msg(fd, OPERATION_COUNTER, current) != 0) {
                    // 傳失敗就當作連線結束
                }

                // Server 也要把 counter 印到 stdout
                {
                    std::lock_guard<std::mutex> lock(print_mutex);
                    std::cout << current << std::endl;
                    std::cout.flush();
                }

                // TERMINATION 後關閉連線並移除
                close(fd);
                to_remove.push_back(fd);
            } else {
                // 未知 op，直接關閉
                close(fd);
                to_remove.push_back(fd);
            }
        }

        // 5) 從 worker 的 sockets 列表移除已關閉的 fd
        if (!to_remove.empty()) {
            std::lock_guard<std::mutex> lock(worker->sockets_mutex);
            for (int fd_close : to_remove) {
                auto it = std::remove(worker->sockets.begin(),
                                      worker->sockets.end(),
                                      fd_close);
                worker->sockets.erase(it, worker->sockets.end());
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "usage: ./server <numThreads> <port>\n";
        return 1;
    }

    int numThreads = std::atoi(argv[1]);
    int port = std::atoi(argv[2]);

    if (numThreads <= 0) {
        std::cerr << "numThreads must be > 0\n";
        return 1;
    }

    // 1) 建立 listening socket
    int listen_fd = listening_socket(port);
    if (listen_fd < 0) {
        std::cerr << "failed to create listening socket on port " << port << "\n";
        return 1;
    }

    // 2) 建立 worker threads
    std::vector<Worker> workers(numThreads);

    for (int i = 0; i < numThreads; ++i) {
        int pipefd[2];
        if (pipe(pipefd) != 0) {
            std::cerr << "failed to create pipe for worker " << i << "\n";
            return 1;
        }
        workers[i].wakeup_read_fd = pipefd[0];
        workers[i].wakeup_write_fd = pipefd[1];

        workers[i].thread = std::thread(worker_loop, &workers[i]);
    }

    // 3) 主 thread：接受連線並分配給 workers
    int64_t connection_count = 0;

    while (true) {
        int client_fd = accept_connection(listen_fd);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            // 可以選擇印錯誤到 stderr；stdout 只印 counter
            std::cerr << "accept failed\n";
            continue;
        }

        int idx = 0;
        if (numThreads > 0) {
            idx = static_cast<int>(connection_count % numThreads);
        }
        ++connection_count;

        Worker &w = workers[idx];

        // 新連線加入 worker 的 new_connections
        {
            std::lock_guard<std::mutex> lock(w.new_conn_mutex);
            w.new_connections.push(client_fd);
        }

        // 通知 worker，有新的 fd 要處理
        char c = 'x';
        ssize_t n = write(w.wakeup_write_fd, &c, 1);
        (void)n; // 忽略寫入失敗情況，最差下次 select 超時再處理
    }

    // 理論上 server 是 long-running，不會走到這裡
    // 不過為了完整性，若日後要優雅關閉，可在此 join threads。
    // for (auto &w : workers) {
    //     if (w.thread.joinable()) {
    //         w.thread.join();
    //     }
    // }

    return 0;
}
