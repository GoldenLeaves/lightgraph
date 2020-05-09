//
// Created by tramboo on 2020/3/26.
//

#include <iostream>
#include <memory>

class base {
public:
    virtual int next() = 0;
    virtual int next(int a) = 0;

    virtual ~base() {
        std::cout << "destruction: base" << std::endl;
    };
};

class son_a: public base {
public:
    int next() override {
        return 2;
    }

    int next(int a) override {
        return 2 + a;
    }

    ~son_a() override {
        std::cout << "destruction: son" << std::endl;
    };
};

std::unique_ptr<base> Get() {
    std::unique_ptr<son_a> son(new son_a);
    return son;
}

int main() {
    auto x = Get();
    std::cout << "Got!" << std::endl;
    std::cout << x->next() << std::endl;
    std::cout << x->next(4) << std::endl;
    return 0;
}

