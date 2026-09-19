#pragma once

#include "trading/events.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace trading {

enum class EnqueueResult {
    accepted,
    full,
    closed,
};

// What a full mailbox does with a new event. Neither policy ever blocks the publisher.
// reject_newest suits signals and executions: the loss is reported to the publisher.
// drop_oldest suits market data: a lagging consumer skips stale quotes and keeps the
// freshest ones, and the eviction is counted by the mailbox.
enum class OverflowPolicy {
    reject_newest,
    drop_oldest,
};

class IEventSink {
public:
    virtual ~IEventSink() = default;
    virtual EnqueueResult try_enqueue(const EventEnvelope& event) = 0;
};

struct MailboxStats {
    std::size_t depth{};
    std::size_t capacity{};
    std::size_t max_depth{};
    std::uint64_t accepted{};
    std::uint64_t rejected_full{};
    std::uint64_t evicted_oldest{};
    std::uint64_t rejected_closed{};
};

// Bounded multi-producer, single-consumer queue. Slots are allocated once, so enqueueing
// never allocates; the per-subscriber cost of a publish is one shared-pointer copy.
class BoundedMailbox final : public IEventSink {
public:
    explicit BoundedMailbox(std::size_t capacity,
                            OverflowPolicy policy = OverflowPolicy::reject_newest);

    EnqueueResult try_enqueue(const EventEnvelope& event) override;

    // Blocks until at least one event is queued, then moves up to max_batch events into
    // batch under one lock acquisition. Returns false once the mailbox is closed and empty.
    // Events handed out in a batch no longer count towards the capacity.
    bool wait_pop_batch(std::vector<EventEnvelope>& batch, std::size_t max_batch);
    bool try_pop(EventEnvelope& event);
    void close();

    [[nodiscard]] std::size_t size() const;
    [[nodiscard]] MailboxStats stats() const;

private:
    void pop_front_locked(EventEnvelope& event);

    const OverflowPolicy policy_;
    mutable std::mutex mutex_;
    std::condition_variable available_;
    std::vector<EventEnvelope> slots_;
    std::size_t head_{0};
    std::size_t size_{0};
    std::size_t waiting_consumers_{0};
    bool closed_{false};
    std::size_t max_depth_{0};
    std::uint64_t accepted_{0};
    std::uint64_t rejected_full_{0};
    std::uint64_t evicted_oldest_{0};
    std::uint64_t rejected_closed_{0};
};

struct PublishResult {
    std::size_t subscribers{};
    std::size_t delivered{};
    std::size_t rejected_full{};
    std::size_t rejected_closed{};

    [[nodiscard]] bool all_delivered() const noexcept { return delivered == subscribers; }
};

struct EventBusStats {
    std::uint64_t events_published{};
    std::uint64_t events_without_subscribers{};
    std::size_t subscriptions{};
    std::size_t topics{};
};

// One registered subscription, for diagnostics and the dashboard topology view.
struct RouteInfo {
    std::string event_type;
    std::string topic;  // "*" for every topic
    std::string subscriber;
};

// In-process publish/subscribe bus. Routing is by (event type, topic); a subscriber either
// takes one topic or every topic of a type. The route table is read under a shared lock
// for the whole delivery, which makes unsubscribe() a barrier: once it returns, no publish
// is delivering to that sink. Delivery never blocks because sinks only try_enqueue.
class EventBus {
public:
    // Unsubscribes on destruction. Must not outlive the bus that issued it.
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
        Subscription(EventBus* bus, std::uint64_t id) noexcept;

        EventBus* bus_{nullptr};
        std::uint64_t id_{};
    };

    EventBus() = default;
    ~EventBus() = default;
    EventBus(const EventBus&) = delete;
    EventBus& operator=(const EventBus&) = delete;

    // Returns the topic registered under name, registering it on first use.
    [[nodiscard]] Topic topic(std::string_view name);
    [[nodiscard]] std::string topic_name(Topic topic) const;

    // Receives every event of type E, whatever its topic (including untagged events).
    template <BusEvent E>
    [[nodiscard]] Subscription subscribe(std::shared_ptr<IEventSink> sink,
                                         std::string subscriber = {}) {
        return subscribe_impl(typeid(E), E::kEventName, std::nullopt, std::move(sink),
                              std::move(subscriber));
    }

    // Receives only events of type E published on topic.
    template <BusEvent E>
    [[nodiscard]] Subscription subscribe(std::shared_ptr<IEventSink> sink,
                                         Topic topic,
                                         std::string subscriber = {}) {
        return subscribe_impl(typeid(E), E::kEventName, topic, std::move(sink),
                              std::move(subscriber));
    }

    template <typename E>
        requires BusEvent<std::remove_cvref_t<E>>
    PublishResult publish(E&& event, Topic topic = {}) {
        using Stored = std::remove_cvref_t<E>;
        return publish_impl(typeid(Stored), topic,
                            std::make_shared<const Stored>(std::forward<E>(event)));
    }

    [[nodiscard]] std::vector<RouteInfo> routes() const;
    [[nodiscard]] EventBusStats stats() const;

private:
    struct RouteKey {
        std::type_index type;
        std::uint32_t topic;

        friend bool operator==(const RouteKey&, const RouteKey&) = default;
    };

    struct RouteKeyHash {
        std::size_t operator()(const RouteKey& key) const noexcept {
            return key.type.hash_code() ^ (std::size_t{key.topic} * 0x9E3779B97F4A7C15ULL);
        }
    };

    struct SubscriberRecord {
        std::uint64_t id{};
        std::type_index type{typeid(void)};
        std::string_view type_name;
        std::optional<Topic> topic;
        std::shared_ptr<IEventSink> sink;
        std::string name;
    };

    using SinkList = std::vector<std::shared_ptr<IEventSink>>;

    Subscription subscribe_impl(std::type_index type,
                                std::string_view type_name,
                                std::optional<Topic> topic,
                                std::shared_ptr<IEventSink> sink,
                                std::string subscriber);
    PublishResult publish_impl(std::type_index type,
                               Topic topic,
                               std::shared_ptr<const void> payload);
    void unsubscribe(std::uint64_t id);
    void rebuild_routes_locked(std::type_index type);

    // Guards everything below it. Publishers take it shared; (un)subscribe and topic
    // registration take it exclusively and are expected only at wiring and shutdown time.
    mutable std::shared_mutex mutex_;
    std::vector<SubscriberRecord> records_;
    // Each route already merges topic subscribers with every-topic subscribers, so a
    // publish performs one lookup. Topic 0 holds the every-topic subscribers alone.
    std::unordered_map<RouteKey, SinkList, RouteKeyHash> routes_;
    std::unordered_map<std::string, Topic> topic_ids_;
    std::vector<std::string> topic_names_;
    std::uint64_t next_subscription_id_{1};

    std::atomic<std::uint64_t> events_published_{0};
    std::atomic<std::uint64_t> events_without_subscribers_{0};
};

}  // namespace trading
