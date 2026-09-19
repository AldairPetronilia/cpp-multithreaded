#pragma once

#include "trading/events.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace trading {

enum class EnqueueResult {
    accepted,
    full,
    closed,
};

class IEventSink {
public:
    virtual ~IEventSink() = default;
    virtual EnqueueResult try_enqueue(const EventEnvelope& event) = 0;
};

class BoundedMailbox final : public IEventSink {
public:
    explicit BoundedMailbox(std::size_t capacity);

    EnqueueResult try_enqueue(const EventEnvelope& event) override;
    bool wait_pop(EventEnvelope& event);
    bool try_pop(EventEnvelope& event);
    void close();

    [[nodiscard]] bool closed() const;
    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] std::size_t max_depth() const noexcept;
    [[nodiscard]] std::uint64_t rejected_full() const noexcept;

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable available_;
    std::deque<EventEnvelope> queue_;
    bool closed_{false};
    std::atomic<std::size_t> max_depth_{0};
    std::atomic<std::uint64_t> rejected_full_{0};
};

struct PublishResult {
    std::size_t subscribers{};
    std::size_t delivered{};
    std::size_t rejected_full{};
    std::size_t rejected_closed{};

    [[nodiscard]] bool all_delivered() const noexcept {
        return delivered == subscribers;
    }
};

struct EventBusStats {
    std::uint64_t events_published{};
    std::uint64_t subscriber_deliveries{};
    std::uint64_t rejected_full{};
    std::uint64_t rejected_closed{};
    std::uint64_t events_without_subscribers{};
};

class EventBus {
public:
    class Subscription {
    public:
        Subscription() = default;
        ~Subscription();
        Subscription(const Subscription&) = delete;
        Subscription& operator=(const Subscription&) = delete;
        Subscription(Subscription&& other) noexcept;
        Subscription& operator=(Subscription&& other) noexcept;

        void reset();

    private:
        friend class EventBus;
        Subscription(EventBus* bus, std::type_index type, std::uint64_t id);

        EventBus* bus_{nullptr};
        std::type_index type_{typeid(void)};
        std::uint64_t id_{};
    };

    EventBus() = default;
    ~EventBus() = default;
    EventBus(const EventBus&) = delete;
    EventBus& operator=(const EventBus&) = delete;

    template <typename EventType>
    Subscription subscribe(const std::shared_ptr<IEventSink>& sink) {
        static_assert(std::is_base_of_v<Event, EventType>);
        return subscribe_impl(std::type_index(typeid(EventType)), sink);
    }

    template <typename EventType>
    PublishResult publish(EventType&& event) {
        using StoredEvent = std::remove_cvref_t<EventType>;
        static_assert(std::is_base_of_v<Event, StoredEvent>);
        auto payload = std::make_shared<const StoredEvent>(std::forward<EventType>(event));
        return publish_impl(std::type_index(typeid(StoredEvent)), std::move(payload));
    }

    [[nodiscard]] EventBusStats stats() const noexcept;

private:
    struct Subscriber {
        std::uint64_t id{};
        std::weak_ptr<IEventSink> sink;
    };

    Subscription subscribe_impl(std::type_index type,
                                const std::shared_ptr<IEventSink>& sink);
    PublishResult publish_impl(std::type_index type,
                               std::shared_ptr<const Event> payload);
    void unsubscribe(std::type_index type, std::uint64_t id);

    mutable std::shared_mutex subscriptions_mutex_;
    std::unordered_map<std::type_index, std::vector<Subscriber>> subscriptions_;
    std::atomic<std::uint64_t> next_subscription_id_{1};
    std::atomic<std::uint64_t> next_event_sequence_{1};
    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> subscriber_deliveries_{0};
    std::atomic<std::uint64_t> rejected_full_{0};
    std::atomic<std::uint64_t> rejected_closed_{0};
    std::atomic<std::uint64_t> events_without_subscribers_{0};
};

}  // namespace trading

