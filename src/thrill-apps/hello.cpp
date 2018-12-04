#include <thrill/api/context.hpp>

int main(int argc, const char** argv) {
    return thrill::Run([&](thrill::Context& ctx) {
        LOG1 << "Hello!";
    });
}
