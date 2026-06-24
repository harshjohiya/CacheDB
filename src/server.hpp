#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "thread_pool.hpp"

struct CacheValue
{
    std::string data;
    std::chrono::time_point<std::chrono::steady_clock> expires_at;
    bool has_ttl = false;
};

class Server
{
public:
    Server(int port);
    ~Server();

    void start();

private:
    int port_;
    int server_fd_;

    ThreadPool thread_pool_;
    std::unordered_map<std::string, CacheValue> store_;
    std::shared_mutex store_mutex_;
    std::mutex aof_mutex_;
    std::ofstream aof_stream_;

    std::thread eviction_thread_;
    std::atomic<bool> stop_eviction_{ false };
    std::atomic<pid_t> bgsave_pid_{ -1 };

    std::vector<int> replica_fds_;
    std::shared_mutex replica_mutex_;
    std::thread replica_worker_;
    std::atomic<bool> is_replica_{ false };

    void connect_to_master(std::string host, int port);
    void broadcast_to_replicas(const std::string& resp_payload);

    void eviction_loop();
    void load_aof();
    void handle_client(int client_fd);

    std::vector<std::string> parse_resp(const std::string& input,
                                        size_t& consumed);
    std::string process_command(const std::vector<std::string>& args,
                                int client_fd = -1);
};