#include "trading/messaging.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace trading {

BoundedMailbox::BoundedMailbox(std::size_t capacity, OverflowPolicy policy)
    : policy_(policy) {
    if (capacity == 0U) {
        throw std::invalid_argument("mailbox capacity must be greater than zero");
    }
    slots_.resize(capacity);
}

EnqueueResult BoundedMailbox::try_enqueue(const EventEnvelope& event) {
    // Declared before the lock so an evicted payload is released outside the critical section.
    EventEnvelope evicted;
    std::unique_lock lock(mutex_);
    if (closed_) {
        ++rejected_closed_;
        return EnqueueResult::closed;
    }
    if (size_ == slots_.size()) {
        if (policy_ == OverflowPolicy::reject_newest) {
            ++rejected_full_;
            return EnqueueResult::full;
        }
        pop_front_locked(evicted);
        ++evicted_oldest_;
    }

    auto tail = head_ + size_;
    if (tail >= slots_.size()) {
        tail -= slots_.size();
    }
    slots_[tail] = event;
    ++size_;
    ++accepted_;
    max_depth_ = std::max(max_depth_, size_);
    const bool consumer_waiting = waiting_consumers_ > 0U;
    lock.unlock();
    // A busy consumer re-checks the queue before waiting, so it needs no notification.
    if (consumer_waiting) {
        available_.notify_one();
    }
    return EnqueueResult::accepted;
}

bool BoundedMailbox::wait_pop_batch(std::vector<EventEnvelope>& batch, std::size_t max_batch) {
    batch.clear();
    std::unique_lock lock(mutex_);
    ++waiting_consumers_;
    available_.wait(lock, [this] { return closed_ || size_ > 0U; });
    --waiting_consumers_;
    if (size_ == 0U) {
        return false;
    }
    const auto count = std::min(size_, std::max<std::size_t>(max_batch, 1U));
    for (std::size_t index = 0; index < count; ++index) {
        pop_front_locked(batch.emplace_back());
    }
    return true;
}

bool BoundedMailbox::try_pop(EventEnvelope& event) {
    std::lock_guard lock(mutex_);
    if (size_ == 0U) {
        return false;
    }
    pop_front_locked(event);
    return true;
}

void BoundedMailbox::pop_front_locked(EventEnvelope& event) {
    // Moving out leaves the slot's payload pointer empty, so the ring never pins events.
    event = std::move(slots_[head_]);
    if (++head_ == slots_.size()) {
        head_ = 0;
    }
    --size_;
}

void BoundedMailbox::close() {
    {
        std::lock_guard lock(mutex_);
        closed_ = true;
    }
    available_.notify_all();
}

std::size_t BoundedMailbox::size() const {
    std::lock_guard lock(mutex_);
    return size_;
}

MailboxStats BoundedMailbox::stats() const {
    std::lock_guard lock(mutex_);
    return MailboxStats{
        size_,
        slots_.size(),
        max_depth_,
        accepted_,
        rejected_full_,
        evicted_oldest_,
        rejected_closed_,
    };
}

EventBus::Subscription::Subscription(EventBus* bus, std::uint64_t id) noexcept
    : bus_(bus), id_(id) {}

EventBus::Subscription::~Subscription() {
    reset();
}

EventBus::Subscription::Subscription(Subscription&& other) noexcept
    : bus_(std::exchange(other.bus_, nullptr)), id_(std::exchange(other.id_, 0)) {}

EventBus::Subscription& EventBus::Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        reset();
        bus_ = std::exchange(other.bus_, nullptr);
        id_ = std::exchange(other.id_, 0);
    }
    return *this;
}

void EventBus::Subscription::reset() {
    if (bus_ != nullptr) {
        bus_->unsubscribe(id_);
        bus_ = nullptr;
        id_ = 0;
    }
}

Topic EventBus::topic(std::string_view name) {
    if (name.empty()) {
        throw std::invalid_argument("topic name must not be empty");
    }
    std::unique_lock lock(mutex_);
    const auto found = topic_ids_.find(std::string(name));
    if (found != topic_ids_.end()) {
        return found->second;
    }
    topic_names_.emplace_back(name);
    const Topic registered{static_cast<std::uint32_t>(topic_names_.size())};
    topic_ids_.emplace(std::string(name), registered);
    return registered;
}

std::string EventBus::topic_name(Topic topic) const {
    if (topic.id == 0U) {
        return {};
    }
    std::shared_lock lock(mutex_);
    return topic.id <= topic_names_.size() ? topic_names_[topic.id - 1U] : std::string{};
}

EventBus::Subscription EventBus::subscribe_impl(std::type_index type,
                                                std::string_view type_name,
                                                std::optional<Topic> topic,
                                                std::shared_ptr<IEventSink> sink,
                                                std::string subscriber) {
    if (!sink) {
        throw std::invalid_argument("event sink must not be null");
    }
    std::unique_lock lock(mutex_);
    if (topic && (topic->id == 0U || topic->id > topic_names_.size())) {
        throw std::invalid_argument("subscriptions must name a registered topic");
    }
    const auto id = next_subscription_id_++;
    records_.push_back(SubscriberRecord{
        id, type, type_name, topic, std::move(sink), std::move(subscriber)});
    rebuild_routes_locked(type);
    return Subscription(this, id);
}

void EventBus::unsubscribe(std::uint64_t id) {
    std::unique_lock lock(mutex_);
    const auto found = std::find_if(records_.begin(), records_.end(),
                                    [id](const SubscriberRecord& record) { return record.id == id; });
    if (found == records_.end()) {
        return;
    }
    const auto type = found->type;
    records_.erase(found);
    rebuild_routes_locked(type);
}

void EventBus::rebuild_routes_locked(std::type_index type) {
    std::erase_if(routes_, [type](const auto& route) { return route.first.type == type; });

    SinkList every_topic;
    for (const auto& record : records_) {
        if (record.type == type && !record.topic) {
            every_topic.push_back(record.sink);
        }
    }
    for (const auto& record : records_) {
        if (record.type != type || !record.topic) {
            continue;
        }
        auto [route, inserted] = routes_.try_emplace(RouteKey{type, record.topic->id});
        if (inserted) {
            route->second = every_topic;
        }
        route->second.push_back(record.sink);
    }
    if (!every_topic.empty()) {
        routes_.insert_or_assign(RouteKey{type, 0U}, std::move(every_topic));
    }
}

PublishResult EventBus::publish_impl(std::type_index type,
                                     Topic topic,
                                     std::shared_ptr<const void> payload) {
    events_published_.fetch_add(1, std::memory_order_relaxed);

    std::shared_lock lock(mutex_);
    auto found = routes_.find(RouteKey{type, topic.id});
    if (found == routes_.end() && topic.id != 0U) {
        found = routes_.find(RouteKey{type, 0U});
    }
    if (found == routes_.end()) {
        lock.unlock();
        events_without_subscribers_.fetch_add(1, std::memory_order_relaxed);
        return {};
    }

    const EventEnvelope envelope{type, topic, MonotonicClock::now(), std::move(payload)};
    PublishResult result;
    result.subscribers = found->second.size();
    for (const auto& sink : found->second) {
        switch (sink->try_enqueue(envelope)) {
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
    return result;
}

std::vector<RouteInfo> EventBus::routes() const {
    std::shared_lock lock(mutex_);
    std::vector<RouteInfo> routes;
    routes.reserve(records_.size());
    for (const auto& record : records_) {
        routes.push_back(RouteInfo{
            std::string(record.type_name),
            record.topic ? topic_names_[record.topic->id - 1U] : std::string("*"),
            record.name,
        });
    }
    return routes;
}

EventBusStats EventBus::stats() const {
    std::shared_lock lock(mutex_);
    return EventBusStats{
        events_published_.load(std::memory_order_relaxed),
        events_without_subscribers_.load(std::memory_order_relaxed),
        records_.size(),
        topic_names_.size(),
    };
}

}  // namespace trading
