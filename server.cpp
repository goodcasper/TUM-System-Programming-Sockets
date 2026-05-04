#include <atomic>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <cstdlib>
#include <unistd.h>     
#include <sys/select.h>  
#include <errno.h>
#include <algorithm>

#include "utils.h"

// 全域 counter 
// 原子：確保操作這個變數的時候 是不會被中斷的    原子保護單一變數 mutex保護一段程式碼
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
    std::queue<int> new_connections;      // 尚未處理的
};

// worker thread 主迴圈：使用 select() 同時處理多個連線
void worker_loop(Worker *worker) {
    while (true) {
        // 1) 把 main thread 指派的新連線搬進 sockets，new_connections（暫存區）→ sockets（正式工作清單）
        {
            std::lock_guard<std::mutex> lock(worker->new_conn_mutex); // 保護work thread 跟 main thread
            while (!worker->new_connections.empty()) {
                int fd = worker->new_connections.front();
                worker->new_connections.pop();
                std::lock_guard<std::mutex> lock2(worker->sockets_mutex);
                worker->sockets.push_back(fd);
            }
        }

        // 2) 準備 select 的 fd_set
        fd_set readfds; // 監聽清單
        FD_ZERO(&readfds); // 清空

        int maxfd = worker->wakeup_read_fd;
        FD_SET(worker->wakeup_read_fd, &readfds); // select() 會卡住等待，但如果 main thread 分配了新連線進來，需要通知 worker thread 趕快去處理 就是用wakeup_read_fd 不然會一直卡在select等新訊息

        std::vector<int> snapshot;
        {
            std::lock_guard<std::mutex> lock(worker->sockets_mutex);
            snapshot = worker->sockets;  // 做一份副本，避免 select 期間持有鎖
        }

        for (int fd : snapshot) { // 把socket有的都加到監聽清單
            FD_SET(fd, &readfds);
            if (fd > maxfd) {
                maxfd = fd; // 找所有 fd 裡面最大的值，因為 select() 的第一個參數需要傳入最大的 fd + 1
            }
        }

        // 同時監聽很多個 fd，只要其中任何一個有資料，就醒來告訴你是哪個，原本是卡住的狀態
        int ret = select(maxfd + 1, &readfds, nullptr, nullptr, nullptr);
        if (ret < 0) {
            if (errno == EINTR) {
                continue; // 被 signal 打斷就重來
            }
            // 其他錯誤
            continue;
        }

        // FD_ISSET 檢查某個 fd 是否有事件發生
        // 若 wakeup pipe 有資料，讀掉以清除喚醒信號
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

        // 處理每個有資料可讀的 client socket
        std::vector<int> to_remove;  // 結束的連線要從 sockets 列表移除

        for (int fd : snapshot) { // 遍歷每個被監聽的連線
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
                number.fetch_add(arg, std::memory_order_relaxed); // atomic 提供的函式，意思是原子地把值加上 arg。
                // memory_order 就是用來控制允許重排到什麼程度 (編譯器會為了效率而重排指令)，relaxed就是只要保證操作是原子的就好
            } else if (op_type == OPERATION_SUB) {
                number.fetch_sub(arg, std::memory_order_relaxed);
            } else if (op_type == OPERATION_TERMINATION) {
                // 取得目前 counter
                int64_t current = number.load(std::memory_order_relaxed);

               
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
                                      fd_close); // 把要移除的移動到尾端 並回傳該位置
                worker->sockets.erase(it, worker->sockets.end()); // 刪除從起始到結束之間的所有元素
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

    // 建立 listening socket
    int listen_fd = listening_socket(port);
    if (listen_fd < 0) {
        std::cerr << "failed to create listening socket on port " << port << "\n";
        return 1;
    }

    // 建立 worker threads
    std::vector<Worker> workers(numThreads);

    for (int i = 0; i < numThreads; ++i) {
        int pipefd[2];
        if (pipe(pipefd) != 0) { // pipe() 會建立兩個 fd (os分配)，存在 pipefd 陣列裡
            std::cerr << "failed to create pipe for worker " << i << "\n";
            return 1;
        }
        workers[i].wakeup_read_fd = pipefd[0];
        workers[i].wakeup_write_fd = pipefd[1];

        workers[i].thread = std::thread(worker_loop, &workers[i]); // 第一個參數為這個 thread 要執行的函式 後面是這個函式的參數
    }

    // 主thread：接受連線並分配給 workers
    int64_t connection_count = 0;

    while (true) {
        int client_fd = accept_connection(listen_fd); // 卡住等待 直到client發起連線
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            // 可以選擇印錯誤到 stderr；stdout 只印 counter
            std::cerr << "accept failed\n";
            continue;
        }

        int idx = 0; // 看要分配到哪個worker
        if (numThreads > 0) {
            idx = static_cast<int>(connection_count % numThreads);
        }
        ++connection_count;

        Worker &w = workers[idx]; // // w 就是 workers[idx]，兩個名字指向同一個東西

        // 新連線加入 worker 的 new_connections
        {
            std::lock_guard<std::mutex> lock(w.new_conn_mutex);
            w.new_connections.push(client_fd);
        }

        // 通知 worker，有新的 fd 要處理
        char c = 'x';
        ssize_t n = write(w.wakeup_write_fd, &c, 1); // 第一個參數是寫入的管道(fd) 第二個是寫入資料的記憶體位址 第三個是要寫入的bytes
        (void)n; // 忽略寫入失敗情況，最差下次 select 超時再處理
    }

    

    return 0;
}
