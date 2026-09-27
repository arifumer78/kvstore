#include <iostream>
#include <unordered_map>
#include <string>
#include <optional>

/*
 A key-value store is a place where user/applications can store/retrive data. 
 what needs to be considered is how to access data and how to store data and how to make
 it secure by programming e.g. making things private, const properly.
*/
class KVStore{
    public:
    void set(std::string key, std::string value){
        data_.insert_or_assign(std::move(key), std::move(value)).second;
    }

    std::optional<std::string> get(const std::string& key) const{
        if(auto search = data_.find(key); search != data_.end()){
            return search->second;
        }

        return std::nullopt;
    } //this const means we wont be changing the internal state of the class.

    bool del(const std::string& key){
        auto num_elements = data_.erase(key); //returns 0/1 number of elements remvoed.
        
        return (num_elements > 0);
    } //since we are deleting things so it amounds to change of internal state.

    private:
    std::unordered_map<std::string, std::string> data_;
};

int main(int argc, char** argv){
    std::cout<< "initializing kv store"<<std::endl;
    KVStore kv;

    //set
    kv.set("cpp", "is tough");
    if(auto lkup = kv.get("cpp"); lkup != std::nullopt){
        std::cout<<" value = "<<lkup.value()<<std::endl;
    }
    //reset
    kv.set("cpp", "is not so tough");
    if(auto lkup = kv.get("cpp"); lkup != std::nullopt){
        std::cout<<" value = "<<lkup.value()<<std::endl;
    }
    //del
    kv.del("cpp");
    if(auto lkup = kv.get("cpp"); lkup != std::nullopt){
        std::cout<<" value = "<<lkup.value()<<std::endl;
    }
    else{
        std::cout<<" value not found"<<std::endl;
    }

    return 0;
}