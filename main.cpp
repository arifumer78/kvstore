#include <iostream>
#include <optional>
#include <unordered_map>
#include <string>
#include <string_view>
#include <system_error>
#include <expected>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <fcntl.h>
#include <sys/epoll.h>

class KVStore{
    private:
    mutable std::shared_mutex m_;
    std::unordered_map<std::string, std::string> store_;
    public:
    void set(std::string key, std::string value){
        std::unique_lock lk{m_};
        store_.insert_or_assign(std::move(key), std::move(value));
    }

    std::optional<std::string> get(const std::string& key) const {
        std::shared_lock sk{m_};
        if(auto val = store_.find(key); val != store_.end()){
            return val->second;
        }
        return std::nullopt;
    }

    bool del(const std::string& key){
        std::unique_lock uk{m_};
        auto elem = store_.erase(key);
        return (elem > 0);
    }
};

class Socket{
    private:
    int fd_;
    public:
    explicit Socket(int fd) noexcept : fd_{fd} {}
    ~Socket(){if(fd_ >= 0) ::close(fd_);}

    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    Socket(Socket&& other) noexcept : fd_{other.fd_} {other.fd_ = -1;}
    Socket& operator=(Socket&& other) noexcept {if(this != &other){::close(fd_); fd_ = other.fd_; other.fd_ = -1;}   return *this;}
    int get(void) const { return fd_;}
};

[[noreturn]] inline void throw_errno(const char* what){
    throw std::system_error(errno, std::generic_category(), what);
}

class Listener{
    private:
    Socket socket_;
    explicit Listener(Socket s) noexcept : socket_{std::move(s)} {}
    static std::unexpected<std::error_code> fail(){
        return std::unexpected(std::error_code(errno, std::generic_category()));
    }
    public:
    [[nodiscard]] static std::expected<Listener, std::error_code> create(std::uint16_t port){
        Socket s{::socket(AF_INET, SOCK_STREAM, 0)};
        if(0 > s.get()){
            return fail();
        }

        int opt = 1;
        if(0 > setsockopt(s.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))){
            return fail();
        }

        struct sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);
        if(0 > ::bind(s.get(), reinterpret_cast<struct sockaddr*>(&address), sizeof(address) ) ) {
            return fail();
        }

        if(0 > ::listen(s.get(), SOMAXCONN)){
            return fail();
        }
        return Listener{std::move(s)};
    }

    // add inside Listener, public:
    int fd() const noexcept { return socket_.get(); }

    Socket accept(){
        while(true){
            int fd = ::accept(socket_.get(), nullptr, nullptr);
            if(fd >= 0) { return Socket{fd}; }
            if(errno != EINTR && errno != ECONNABORTED) throw_errno("accept()");
        }
    }
};

bool write_all(int fd, std::string_view data){
    while(!data.empty()){
        ssize_t n = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
        if(n < 0){
            if (errno == EINTR) continue;
            return false;
        }
        data.remove_prefix(static_cast<std::size_t>(n));
    }
    return true;
}
 
struct Command {
    enum class Op { Set, Get, Del, Invalid } op = Op::Invalid;
    std::string key;
    std::string value;
};

namespace {
// returns the next space-separated token and advances s past it
std::string_view next_token(std::string_view& s) {
    auto start = s.find_first_not_of(' ');
    if (start == std::string_view::npos) { s = {}; return {}; }
    s.remove_prefix(start);
    auto end = s.find(' ');
    auto tok = s.substr(0, end);
    s.remove_prefix(end == std::string_view::npos ? s.size() : end);
    return tok;
}
}  // namespace

Command parse_command(std::string_view line) {
    std::string_view rest = line;
    auto cmd = next_token(rest);
    auto key = next_token(rest);
    if (key.empty()) return {};

    if (cmd == "GET" || cmd == "DEL") {
        if (!next_token(rest).empty()) return {};          // extra words: invalid
        return {cmd == "GET" ? Command::Op::Get : Command::Op::Del, std::string{key}, {}};
    }
    if (cmd == "SET") {
        auto start = rest.find_first_not_of(' ');
        if (start == std::string_view::npos) return {};   // SET needs a value
        return {Command::Op::Set, std::string{key}, std::string{rest.substr(start)}};
    }
    return {};
}

std::string execute(KVStore& kv, const Command& c) {
    switch (c.op) {
    case Command::Op::Set:
        kv.set(c.key, c.value);
        return "OK\n";
    case Command::Op::Get:
        if (auto v = kv.get(c.key)) return *v + '\n';
        return "NOT_FOUND\n";
    case Command::Op::Del:
        return kv.del(c.key) ? "OK\n" : "NOT_FOUND\n";
    case Command::Op::Invalid:
        break;
    }
    return "ERR usage: SET key value | GET key | DEL key\n";
}

struct Connection{
    Socket socket_;
    std::string buf;
};

class KVServer{
    private:
    static constexpr std::size_t kMaxLine = 64 * 1024;
    KVStore store_;
    Listener listener_;
    Socket epoll_;
    std::unordered_map<int, Connection> clients_;

    bool set_nonblocking(int fd){
        int flags = ::fcntl(fd, F_SETFL, 0);
        return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
    }

    int watch(int fd){
        epoll_event ev{};
        ev.data.fd = fd;
        ev.events = EPOLLIN;
        return ::epoll_ctl(epoll_.get(), EPOLL_CTL_ADD, fd, &ev) == 0;
    }

    void drop(int fd){
        ::epoll_ctl(epoll_.get(), EPOLL_CTL_DEL, fd, nullptr);
        clients_.erase(fd);
    }

    void serve(Socket client){
        std::string buf;
        char chunk[4096];

        while(true){
            //lets keep reading.
            ssize_t n = ::read(client.get(), chunk, sizeof chunk);
            if(n == 0) return; //the client disconnected
            if(n < 0){
                if (errno != EINTR) return; //an error occured
                continue;
            }
            buf.append(chunk, static_cast<std::size_t>(n));

            std::size_t pos; 
            //lets parse and reply
            while((pos = buf.find('\n')) != std::string::npos){
                std::string_view line{buf.data(), pos};
                if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

                //disabling echo server
                //std::string reply = std::string{line} + '\n'
                std::string reply = execute(store_, parse_command(line));
                if(!write_all(client.get(), reply)) return;
                buf.erase(0, pos+1);
            }

            if (buf.size() > kMaxLine) {                 // no newline in 64 KiB: misbehaving client
                write_all(client.get(), "ERR line too long\n");
                return;
            }            
        }
    }

    void accept_all(){
        while(true){
            int fd = ::accept4(listener_.fd(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if(fd < 0){
                if(errno == EWOULDBLOCK || errno == EAGAIN) return;
                if(errno == EINTR || errno == ECONNABORTED) continue;
                throw_errno("accept4()");
            }
            //now need to add to clients_<int, Connection>
            if(!watch(fd)) continue;
            Socket sock{fd};
            clients_.emplace(fd, Connection{std::move(sock), {}});
        }
    }

    void handle_clients(int fd){
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        Connection& c = it->second;

        bool closed = false;
        char chunk[4096];
        while (true) {                                   // read everything available now
            ssize_t n = ::read(fd, chunk, sizeof chunk);
            if (n > 0) { c.buf.append(chunk, static_cast<std::size_t>(n)); continue; }
            if (n == 0) { closed = true; break; }        // client closed
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // drained for now
            closed = true; break;                        // real error
        }

        std::size_t pos;
        while ((pos = c.buf.find('\n')) != std::string::npos) {
            std::string_view line{c.buf.data(), pos};
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
            std::string reply = execute(store_, parse_command(line));
            c.buf.erase(0, pos + 1);
            if (!write_all(fd, reply)) { drop(fd); return; }
        }

        if (!closed && c.buf.size() > kMaxLine) {
            write_all(fd, "ERR line too long\n");
            closed = true;
        }
        if (closed) drop(fd);

    }

    public:
    explicit KVServer(Listener listener) noexcept : listener_{std::move(listener)}, epoll_{::epoll_create1(EPOLL_CLOEXEC)} {
        if (epoll_.get() < 0) throw_errno("epoll_create1()");
        if (!set_nonblocking(listener_.fd())) throw_errno("fcntl()");
        if (!watch(listener_.fd())) throw_errno("epoll_ctl()");
    }
    void run(){
        while(true){
            std::array<epoll_event, 64> events;
            int n = ::epoll_wait(epoll_.get(), events.data(), static_cast<int>(events.size()), -1);
            if(n < 0){
                if(errno == EINTR){ continue;}
                throw_errno("epoll_wait()");
            }
            for(int i = 0; i < n; ++i){
                int fd = events[i].data.fd;
                if(fd == listener_.fd()) accept_all();
                else handle_clients(fd);
            }
            /*Socket client = listener_.accept();
            std::thread([this, c = std::move(client)]() mutable{
                serve(std::move(c));
            }
            ).detach();*/
            //serve(listener_.accept());
        }
    }
};

int main(){
    std::cout<<"starting kvstore app..."<<std::endl;

    try{
        auto listener = Listener::create(6379);
        if(!listener){
            std::cerr<<"listener error "<< listener.error().message()<<std::endl;
            return 1;
        }
        KVServer server{std::move(*listener)};
        server.run();
    }catch(std::system_error& e){
        std::cerr << e.what() << std::endl;
        return 1;
    }

/*    KVStore app;

    if(auto val = app.get("app"); val != std::nullopt){
        std::cout<<"key [app] returned value "<<val.value()<<std::endl;
    }
    else{
        std::cout<<"no value was found.."<<std::endl;
    }

    app.set("app", "kv");

    if(auto val = app.get("app"); val != std::nullopt){
        std::cout<<"key [app] returned value "<<val.value()<<std::endl;
    }
    else{
        std::cout<<"no value was found.."<<std::endl;
    }
*/   
    return 0;
}