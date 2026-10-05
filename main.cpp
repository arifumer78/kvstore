#include <iostream>
#include <optional>
#include <unordered_map>
#include <unistd.h>

class KVStore{
    private:
    std::unordered_map<std::string, std::string> store_;
    public:
    void set(const std::string& key, const std::string value){
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
    Socket& operttor(Socket&& other) noexcept {if(this != &other){::close(fd_); fd_ = other.fd_; other.fd_ = -1;}   return *this;}

};

class Listener{
    private:
    uint16_t port_;
    public:
    explicit Listener(uint16_t port) noexcept : port_{port} {}
    void Listen() const {
        //read from socket
    }
};

class KVServer{
    private:
    KVStore store_;
    Listener listener_;

    public:
    
};

int main(int argc, char** argv){
    std::cout<<"starting kvstore app..."<<std::endl;
    KVStore app;

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
    
    return 0;
}