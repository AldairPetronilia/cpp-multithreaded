#include "trading/messaging.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace trading {

BoundedMailbox::BoundedMailbox(std::size_t capacity) : capacity_(capacity) {
    if (capacity == 0U) {
        throw std::invalid_argument("mailbox capacity must be greater than zero");
    }
}

EnqueueResult BoundedMailbox::try_enqueue(const EventEnvelope& event) {
    std::unique_lock lock(mutex_);
    if (closed_) {
        return EnqueueResult::closed;
    }
    if (queue_.size() >= capacity_) {
        rejected_full_.fetch_add(1, std::memory_order_relaxed);
        return EnqueueResult::full;
    }

    queue_.push_back(event);
    const auto depth = queue_.size();
    auto previous = max_depth_.load(std::memory_order_relaxed);
    while (depth > previous &&
           !max_depth_.compare_exchange_weak(
               previous, depth, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    lock.unlock();
    available_.notify_one();
    return EnqueueResult::accepted;
}

bool BoundedMailbox::wait_pop(EventEnvelope& event) {
    std::unique_lock lock(mutex_);
    available_.wait(lock, [this] { return closed_ || !queue_.empty(); });
    if (queue_.empty()) {
        return false;
    }
    event = std::move(queue_.front());
    queue_.pop_front();
    return true;
}

bool BoundedMailbox::try_pop(EventEnvelope& event) {
    std::lock_guard lock(mutex_);
    if (queue_.empty()) {
        return false;
    }
    event = std::move(queue_.front());
    queue_.pop_front();
    return true;
}

void BoundedMailbox::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    available_.notify_all();
}

bool BoundedMailbox::closed() const {
    std::lock_guard lock(mutex_);
    return closed_;
}

std::size_t BoundedMailbox::size() const {
    std::lock_guard lock(mutex_);
    return queue_.size();
}

std::size_t BoundedMailbox::capacity() const noexcept {
    return capacity_;
}

std::size_t BoundedMailbox::max_depth() const noexcept {
    return max_depth_.load(std::memory_order_relaxed);
}

std::uint64_t BoundedMailbox::rejected_full() const noexcept {
    return rejected_full_.load(std::memory_order_relaxed);
}

EventBus::Subscription::Subscription(EventBus* bus,
                                     std::type_index type,
                                     std::uint64_t id)
    : bus_(bus), type_(type), id_(id) {}

EventBus::Subscription::~Subscription() {
    reset();
}

EventBus::Subscription::Subscription(Subscription&& other) noexcept
    : bus_(std::exchange(other.bus_, nullptr)),
      type_(other.type_),
      id_(std::exchange(other.id_, 0)) {}

EventBus::Subscription& EventBus::Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        reset();
        bus_ = std::exchange(other.bus_, nullptr);
        type_ = other.type_;
        id_ = std::exchange(other.id_, 0);
    }
    return *this;
}

void EventBus::Subscription::reset() {
    if (bus_ != nullptr) {
        bus_->unsubscribe(type_, id_);
        bus_ = nullptr;
        id_ = 0;
    }
}

EventBus::Subscription EventBus::subscribe_impl(
    std::type_index type,
    const std::shared_ptr<IEventSink>& sink) {
    if (!sink) {
        throw std::invalid_argument("event sink must not be null");
    }
    const auto id = next_subscription_id_.fetch_add(1, std::memory_order_relaxed);
    {
        std::unique_lock lock(subscriptions_mutex_);
        subscriptions_[type].push_back(Subscriber{id, sink});
    }
    return Subscription(this, type, id);
}

PublishResult EventBus::publish_impl(std::type_index type,
                                     std::shared_ptr<const Event> payload) {
    std::vector<std::shared_ptr<IEventSink>> targets;
    {
        std::shared_lock lock(subscriptions_mutex_);
        const auto found = subscriptions_.find(type);
        if (found != subscriptions_.end()) {
            targets.reserve(found->second.size());
            for (const auto& subscriber : found->second) {
                if (auto sink = subscriber.sink.lock()) {
                    targets.push_back(std::move(sink));
                }
            }
        }
    }

    events_published_.fetch_add(1, std::memory_order_relaxed);
    if (targets.empty()) {
        events_without_subscribers_.fetch_add(1, std::memory_order_relaxed);
        return {};
    }

    const EventEnvelope envelope{
        type,
        next_event_sequence_.fetch_add(1, std::memory_order_relaxed),
        MonotonicClock::now(),
        std::move(payload),
    };

    PublishResult result;
    result.subscribers = targets.size();
    for (const auto& target : targets) {
        switch (target->try_enqueue(envelope)) {
            case EnqueueResult::accepted:
                ++result.delivered;
                break;
            case EnqueueResult::full:
                ++result.rejected_full;
                break;
            case EnqueueResult::closed:
                ++result.rejected_closed;
                break;
        }
    }

    subscriber_deliveries_.fetch_add(result.delivered, std::memory_order_relaxed);
    rejected_full_.fetch_add(result.rejected_full, std::memory_order_relaxed);
    rejected_closed_.fetch_add(result.rejected_closed, std::memory_order_relaxed);
    return result;
}

void EventBus::unsubscribe(std::type_index type, std::uint64_t id) {
    std::unique_lock lock(subscriptions_mutex_);
    const auto found = subscriptions_.find(type);
    if (found == subscriptions_.end()) {
        return;
    }
    auto& subscribers = found->second;
    std::erase_if(subscribers, [id](const Subscriber& subscriber) {
        return subscriber.id == id || subscriber.sink.expired();
    });
    if (subscribers.empty()) {
        subscriptions_.erase(found);
    }
}

EventBusStats EventBus::stats() const noexcept {
    return EventBusStats{
        events_published_.load(std::memory_order_relaxed),
        subscriber_deliveries_.load(std::memory_order_relaxed),
        rejected_full_.load(std::memory_order_relaxed),
        rejected_closed_.load(std::memory_order_relaxed),
        events_without_subscribers_.load(std::memory_order_relaxed),
    };
}

}  // namespace trading
