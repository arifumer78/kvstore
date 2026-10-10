#include <iostream>
#include <optional>
#include <unordered_map>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>

class KVStore{
    private:
    std::unordered_map<std::string, std::string> store_;
    public:
    void set(std::string key, std::string value){
        store_.insert_or_assign(std::move(key), std::move(value));
    }

    std::optional<std::string> get(const std::string& key) const {
        if(auto val = store_.find(key); val != store_.end()){
            return val->second;
        }
        return std::nullopt;
    }

    bool del(const std::string& key){
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
    public:
    //since we are setting things up in constructor, which can not return, it can only throw exception
    explicit Listener(uint16_t port) : socket_{::socket(AF_INET, SOCK_STREAM, 0)} {
        if(0 > socket_.get()){
            throw_errno("socket()");
        }

        int opt = 1;
        if(0 > setsockopt(socket_.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))){
            throw_errno("setsockopt()");
        }

        struct sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = INADDR_ANY;
        address.sin_port = htons(port);
        if(0 > ::bind(socket_.get(), reinterpret_cast<struct sockaddr*>(&address), sizeof(address) ) ) {
            throw_errno("bind()");
        }

        if(0 > ::listen(socket_.get(), 100)){
            throw_errno("listen()");
        }
    }

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

class KVServer{
    private:
    static constexpr std::size_t kMaxLine = 64 * 1024;
    KVStore store_;
    Listener listener_;
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

    public:
    explicit KVServer(uint16_t port) : listener_{port} {}
    void run(){
        while(true){
            serve(listener_.accept());
        }
    }
};

int main(){
    std::cout<<"starting kvstore app..."<<std::endl;

    try{
        KVServer server{6379};
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