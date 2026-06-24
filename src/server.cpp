#include "server.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

Server::Server(int port)
    : port_(port)
    , server_fd_(-1)
    , thread_pool_(std::thread::hardware_concurrency())
    , stop_eviction_(false)
    , bgsave_pid_(-1)
    , is_replica_(false)
{
    load_aof();
    aof_stream_.open("mini-redis.aof", std::ios::app);
    eviction_thread_ = std::thread(&Server::eviction_loop, this);
}

Server::~Server()
{
    stop_eviction_ = true;
    if (eviction_thread_.joinable())
    {
        eviction_thread_.join();
    }

    // Safely wind down the replication thread if it was spawned
    is_replica_ = false;
    if (replica_worker_.joinable())
    {
        replica_worker_.join();
    }

    if (aof_stream_.is_open())
    {
        aof_stream_.close();
    }

    // Safely lock and clear replica connections
    std::unique_lock<std::shared_mutex> lock(replica_mutex_);
    for (int fd : replica_fds_)
    {
        if (fd >= 0)
            close(fd);
    }
    replica_fds_.clear();

    if (server_fd_ != -1)
    {
        close(server_fd_);
    }
}

void Server::eviction_loop()
{
    while (!stop_eviction_)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        pid_t current_bgsave = bgsave_pid_.load();
        if (current_bgsave != -1)
        {
            int status;
            pid_t result = waitpid(current_bgsave, &status, WNOHANG);
            if (result == current_bgsave)
            {
                bgsave_pid_.store(-1);
                std::cout
                    << "[BGSAVE] Background snapshot finished successfully.\n";
            }
        }

        auto now = std::chrono::steady_clock::now();
        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        for (auto it = store_.begin(); it != store_.end();)
        {
            if (it->second.has_ttl && it->second.expires_at <= now)
            {
                if (aof_stream_.is_open())
                {
                    std::unique_lock<std::mutex> aof_lock(aof_mutex_);
                    aof_stream_ << "DEL " << it->first << "\n";
                    aof_stream_.flush();
                }
                it = store_.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}

void Server::load_aof()
{
    std::ifstream file("mini-redis.aof");
    if (!file.is_open())
        return;

    std::string line;
    std::streampos last_valid_pos = 0;
    int skipped = 0;

    while (std::getline(file, line))
    {
        if (line.empty())
        {
            last_valid_pos = file.tellg();
            continue;
        }

        std::istringstream iss(line);
        std::string command;
        iss >> command;
        bool valid = false;

        if (command == "SET")
        {
            std::string key, value;
            if (iss >> key && std::getline(iss >> std::ws, value)
                && !key.empty() && !value.empty())
            {
                store_[key] = { value, {}, false };
                valid = true;
            }
        }
        else if (command == "DEL")
        {
            std::string key;
            if (iss >> key && !key.empty())
            {
                store_.erase(key);
                valid = true;
            }
        }
        else if (command == "EXPIRE")
        {
            std::string key;
            int seconds = 0;
            if (iss >> key >> seconds && !key.empty() && seconds > 0)
            {
                auto it = store_.find(key);
                if (it != store_.end())
                {
                    it->second.has_ttl = true;
                    it->second.expires_at = std::chrono::steady_clock::now()
                        + std::chrono::seconds(seconds);
                }
                valid = true;
            }
        }

        if (valid)
            last_valid_pos = file.tellg();
        else
            ++skipped;
    }
    file.close();

    if (skipped > 0)
    {
        std::cerr
            << "[AOF] Warning: " << skipped << " malformed entr"
            << (skipped == 1 ? "y" : "ies")
            << " skipped on replay. Truncating AOF to last valid state.\n";
        std::filesystem::resize_file(
            "mini-redis.aof", static_cast<std::uintmax_t>(last_valid_pos));
    }
}

std::vector<std::string> Server::parse_resp(const std::string& input,
                                            size_t& consumed)
{
    consumed = 0;
    std::vector<std::string> args;
    if (input.empty() || input[0] != '*')
        return args;

    try
    {
        size_t pos = 1;
        size_t crlf = input.find("\r\n", pos);
        if (crlf == std::string::npos)
            return args;

        int num_args = std::stoi(input.substr(pos, crlf - pos));
        pos = crlf + 2;

        for (int i = 0; i < num_args; ++i)
        {
            if (pos >= input.length() || input[pos] != '$')
                return {}; // incomplete
            pos++;
            crlf = input.find("\r\n", pos);
            if (crlf == std::string::npos)
                return {}; // incomplete
            int len = std::stoi(input.substr(pos, crlf - pos));
            pos = crlf + 2;
            if (pos + static_cast<size_t>(len) + 2 > input.length())
                return {}; // incomplete
            args.push_back(input.substr(pos, static_cast<size_t>(len)));
            pos += static_cast<size_t>(len) + 2;
        }
        consumed = pos;
    }
    catch (const std::exception& e)
    {
        std::cerr << "[WARN] parse_resp failed: " << e.what() << "\n";
        return {};
    }
    return args;
}

void Server::start()
{
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
    {
        std::cerr << "[FATAL] socket() failed: " << strerror(errno) << "\n";
        return;
    }
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        std::cerr << "[WARN] setsockopt() failed: " << strerror(errno) << "\n";
    }
    struct sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<uint16_t>(port_));
    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
        std::cerr << "[FATAL] bind() failed on port " << port_ << ": "
                  << strerror(errno) << "\n";
        close(server_fd_);
        server_fd_ = -1;
        return;
    }
    if (listen(server_fd_, SOMAXCONN) < 0)
    {
        std::cerr << "[FATAL] listen() failed: " << strerror(errno) << "\n";
        close(server_fd_);
        server_fd_ = -1;
        return;
    }
    std::cout << "Mini-Redis listening on port " << port_ << "...\n";

    while (true)
    {
        struct sockaddr_in client_address{};
        socklen_t client_addr_len = sizeof(client_address);
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_address,
                               &client_addr_len);
        if (client_fd < 0)
            continue;
        thread_pool_.enqueue(
            [this, client_fd]() { this->handle_client(client_fd); });
    }
}

void Server::broadcast_to_replicas(const std::string& resp_payload)
{
    std::shared_lock<std::shared_mutex> lock(replica_mutex_);
    for (int fd : replica_fds_)
    {
        // Send the exact RESP command to the replica
        if (write(fd, resp_payload.c_str(), resp_payload.length()) < 0)
        {
        }
    }
}

void Server::connect_to_master(std::string host, int port)
{
    int master_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &serv_addr.sin_addr);

    if (connect(master_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
        std::cerr << "Failed to connect to master!\n";
        close(master_fd);
        return;
    }

    std::cout << "Successfully connected to master at " << host << ":" << port
              << "\n";

    // Handshake: Tell the master we are a replica
    std::string handshake = "*1\r\n$12\r\nI_AM_REPLICA\r\n";
    if (write(master_fd, handshake.c_str(), handshake.length()) < 0)
    {
    }

    // Enter a continuous loop to receive and apply broadcasted commands
    std::vector<char> buffer(65536);
    while (is_replica_)
    {
        ssize_t bytes_read = read(master_fd, buffer.data(), buffer.size() - 1);
        if (bytes_read <= 0)
        {
            std::cout << "Master disconnected.\n";
            break;
        }
        buffer[static_cast<size_t>(bytes_read)] = '\0';

        size_t consumed = 0;
        std::vector<std::string> args = parse_resp(
            std::string(buffer.data(), static_cast<size_t>(bytes_read)),
            consumed);
        if (!args.empty())
        {
            process_command(args,
                            -1); // Process silently, don't write back to master
        }
    }
    close(master_fd);
}

void Server::handle_client(int client_fd)
{
    std::vector<char> chunk(4096);
    std::string accum;

    while (true)
    {
        ssize_t bytes_read = read(client_fd, chunk.data(), chunk.size());
        if (bytes_read <= 0)
            break;
        accum.append(chunk.data(), static_cast<size_t>(bytes_read));

        // Process all complete RESP messages in the accumulation buffer
        while (!accum.empty())
        {
            size_t consumed = 0;
            std::vector<std::string> args = parse_resp(accum, consumed);
            if (args.empty() || consumed == 0)
                break; // incomplete message, wait for more data

            std::string raw = accum.substr(0, consumed);
            accum.erase(0, consumed);

            std::string response = process_command(args, client_fd);

            std::string cmd = args[0];
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            if (cmd == "SET" || cmd == "DEL" || cmd == "EXPIRE")
            {
                broadcast_to_replicas(raw);
            }

            if (!response.empty())
            {
                if (write(client_fd, response.c_str(), response.length()) < 0)
                {
                    std::cerr << "[WARN] write() to client failed: "
                              << strerror(errno) << "\n";
                    goto disconnect;
                }
            }
        }
    }
disconnect:

    // If a client disconnects, check if it was a replica and remove it
    std::unique_lock<std::shared_mutex> lock(replica_mutex_);
    replica_fds_.erase(
        std::remove(replica_fds_.begin(), replica_fds_.end(), client_fd),
        replica_fds_.end());

    close(client_fd);
}

std::string Server::process_command(const std::vector<std::string>& args,
                                    int client_fd)
{
    std::string command = args[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    if (command == "PING")
        return "+PONG\r\n";

    else if (command == "I_AM_REPLICA")
    {
        std::unique_lock<std::shared_mutex> lock(replica_mutex_);
        if (client_fd != -1)
            replica_fds_.push_back(client_fd);
        std::cout << "Registered new replica connection.\n";
        return ""; // Return empty so we don't close the socket or reply
    }
    else if (command == "REPLICAOF")
    {
        if (args.size() != 3)
            return "-ERR syntax error\r\n";
        std::string host = args[1];
        int port = 0;
        try
        {
            port = std::stoi(args[2]);
        }
        catch (const std::exception&)
        {
            return "-ERR value is not an integer or out of range\r\n";
        }

        if (is_replica_.exchange(true))
        {
            return "-ERR already a replica\r\n";
        }
        if (replica_worker_.joinable())
        {
            replica_worker_.join();
        }
        replica_worker_ =
            std::thread(&Server::connect_to_master, this, host, port);
        return "+OK\r\n";
    }

    else if (command == "BGSAVE")
    {
        if (bgsave_pid_.load() != -1)
            return "-ERR Background save already in progress\r\n";

        pid_t pid;
        {
            std::unique_lock<std::shared_mutex> lock(store_mutex_);
            pid = fork();
            if (pid == 0)
            {
                std::ofstream rdb("dump.rdb", std::ios::trunc);
                for (const auto& [k, v] : store_)
                    rdb << k << " " << v.data << "\n";
                rdb.flush();
                rdb.close();
                _exit(0);
            }
        } // lock released here before we check pid

        if (pid > 0)
        {
            bgsave_pid_.store(pid);
            return "+Background saving started\r\n";
        }
        return "-ERR Failed to create background process\r\n";
    }

    else if (command == "SET")
    {
        if (args.size() < 3)
            return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        std::string value = args[2];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        store_[key] = { value, {}, false };

        if (aof_stream_.is_open() && !is_replica_)
        {
            std::unique_lock<std::mutex> aof_lock(aof_mutex_);
            aof_stream_ << "SET " << key << " " << value << "\n";
            aof_stream_.flush();
        }
        return "+OK\r\n";
    }
    else if (command == "EXPIRE")
    {
        if (args.size() != 3)
            return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        int seconds = 0;
        try
        {
            seconds = std::stoi(args[2]);
        }
        catch (const std::exception&)
        {
            return "-ERR value is not an integer or out of range\r\n";
        }

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        auto it = store_.find(key);
        if (it != store_.end())
        {
            it->second.has_ttl = true;
            it->second.expires_at = std::chrono::steady_clock::now()
                + std::chrono::seconds(seconds);
            if (aof_stream_.is_open() && !is_replica_)
            {
                std::unique_lock<std::mutex> aof_lock(aof_mutex_);
                aof_stream_ << "EXPIRE " << key << " " << seconds << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n";
        }
        return ":0\r\n";
    }
    else if (command == "GET")
    {
        if (args.size() != 2)
            return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];
        bool expired = false;

        {
            std::shared_lock<std::shared_mutex> lock(store_mutex_);
            auto it = store_.find(key);
            if (it != store_.end())
            {
                if (it->second.has_ttl
                    && it->second.expires_at
                        <= std::chrono::steady_clock::now())
                {
                    expired = true;
                }
                else
                {
                    return "$" + std::to_string(it->second.data.length())
                        + "\r\n" + it->second.data + "\r\n";
                }
            }
            else
            {
                return "$-1\r\n";
            }
        }

        if (expired)
        {
            std::unique_lock<std::shared_mutex> lock(store_mutex_);
            store_.erase(key);
            if (aof_stream_.is_open() && !is_replica_)
            {
                std::unique_lock<std::mutex> aof_lock(aof_mutex_);
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return "$-1\r\n";
        }
        return "$-1\r\n"; // unreachable but satisfies compiler
    }
    else if (command == "DEL")
    {
        if (args.size() != 2)
            return "-ERR wrong number of arguments\r\n";
        std::string key = args[1];

        std::unique_lock<std::shared_mutex> lock(store_mutex_);
        if (store_.erase(key))
        {
            if (aof_stream_.is_open() && !is_replica_)
            {
                std::unique_lock<std::mutex> aof_lock(aof_mutex_);
                aof_stream_ << "DEL " << key << "\n";
                aof_stream_.flush();
            }
            return ":1\r\n";
        }
        return ":0\r\n";
    }

    return "-ERR unknown command\r\n";
}