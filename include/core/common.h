#ifndef ISCADA_TEST_COMMON_H_
#define ISCADA_TEST_COMMON_H_

#include <iostream>
#include <string>
#include <vector>
#include <functional>

namespace ISCADA {
namespace Test {
    struct Command {
        std::vector<std::string> options;
        std::string description;
        std::function<bool(std::vector<std::string>)> action;
        int argCount;

        Command(std::vector<std::string> opts, std::string desc, 
                std::function<bool(std::vector<std::string>)> act, int count)
            : options(std::move(opts)), description(std::move(desc)), 
              action(std::move(act)), argCount(count) {}
    };
}
}

#endif
