#pragma once

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>

#include <functional>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class CallbackNode final : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     */
    template <typename ParentDIA, typename CallbackFunction>
    explicit CallbackNode(
        const ParentDIA& parent,
        CallbackFunction callback)
        : Super(parent.ctx(), "Callback", { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty),
          callback_(callback)
    {
        auto propagate_fn = [this](const ValueType& input) {
                                this->PushItem(input);
                            };
        auto lop_chain = parent.stack().push(propagate_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    bool ForwardDataOnly() const final { return true; }

    bool RequireParentPushData(size_t) const final
    { return true; }

    void Execute() final {
        abort();
    }

    void StartPreOp(size_t /* id */) final {
        for (typename Super::Child& child : Super::children_)
            child.node->StartPreOp(child.parent_index);
    }

    void StopPreOp(size_t /* id */) final {
        callback_();

        for (typename Super::Child& child : Super::children_)
            child.node->StopPreOp(child.parent_index);
    }

    void PushData(bool /* consume */) final {
        abort();
    }

private:
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;
    //! The callback function
    std::function<void()> callback_;
};

namespace ext {
    // This will be a proposed addition to DIA.
    // To stay independent from my experimental Thrill fork, I implement this
    // as a namespace-level function here.
    template <typename ValueType, typename Stack, typename CallbackFunction>
    auto Callback(DIA<ValueType, Stack>&& _this, CallbackFunction callback) {

        assert(_this.IsValid());
        auto node = tlx::make_counting<
            CallbackNode<ValueType>>(_this, callback);
        return DIA<ValueType>(node);
    }

    template <typename ValueType, typename Stack, typename CallbackFunction>
    auto Callback(DIA<ValueType, Stack>& _this, CallbackFunction callback) {

        assert(_this.IsValid());
        auto node = tlx::make_counting<
            CallbackNode<ValueType>>(_this, callback);
        return DIA<ValueType>(node);
    }
}

} // namespace api
} // namespace thrill

