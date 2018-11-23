#pragma once

#include <distwt/common/result.hpp>

#include <thrill/api/context.hpp>

class Result : public ResultBase {
public:
    Result(
        const std::string& algo,
        thrill::Context& ctx,
        const std::string& input_filename,
        const size_t input_size,
        const double time);
};
