#pragma once

#include <thrill/api/dia.hpp>

namespace thrill {
namespace api {
namespace ext {

    // Force Thrill into fully processing the given DIA right now.
    //
    // Use with ReadBinary to make sure the initial input I/O is finished
    // when this function returns.
    template <typename ValueType, typename Stack>
    auto Force(DIA<ValueType, Stack>&& _this) {
        assert(_this.IsValid());
        auto id = _this.Map([&](ValueType x){return x;});
        id.Size();
        return id;
    }

}}} //ns