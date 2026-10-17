#pragma once

#include "trading/api.hpp"
#include "trading/fault.hpp"
#include "trading/fencing.hpp"
#include "trading/journal.hpp"
#include "trading/messaging.hpp"
#include "trading/resilience.hpp"
#include "trading/workers.hpp"

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace trading {

struct StrategyConfig {
    std::string name{};
    std::string kind{"ema-cross"};
    // Instruments whose market-data topics feed this strategy's mailbox; empty means all.
    std::vector<std::string> symbols{};
    double quantity{1.0};
};

// What every supervised component needs from the system that builds it.
struct ComponentContext {
    EventBus& bus;
    Graveyard& graveyard;
    SupervisorConfig supervision;
    // Where state is rebuilt from. Null means a replacement starts cold.
    JournalWriter* journal{nullptr};
    // Rung by a worker as it dies. Null when nothing supervises the component.
    std::shared_ptr<Doorbell> doorbell{};
};

// Recovery helpers, each run on the replacement's own thread before it goes live. They throw
// JournalError when the journal cannot be trusted (it failed, fell behind or is damaged), so
// the replacement dies rather than going live on a partial history.
//
// Reapplies the newest journaled execution reports to the position book. The book outlives
// the monitor, so it is the checkpoint: a replacement only needs the reports its predecessor
// missed, which are the newest ones, and the book recognises the rest by order id. Reapplying
// the whole history would lean on the book's deduplication window, which spans a redelivery,
// not a session: past 65,536 fills the oldest would count twice.
void reapply_recent_reports(JournalWriter& journal, PositionBook& book);

// Each component below follows the same recovery recipe:
//   1. abandon the failed instance (it may be stuck; never wait for it) and bury it,
//   2. advance the generation gate, so the failed instance can no longer write anywhere,
//   3. build a replacement that subscribes first, so live events buffer,
//   4. let it rebuild its state from the journal on its own thread before it goes live,
//   5. swap it in.

// One strategy, its thread, mailbox and signal processor.
class StrategyComponent final : public SupervisedComponent {
public:
    StrategyComponent(const ComponentContext& context, StrategyConfig config, WorkerOptions worker);
    ~StrategyComponent() override;

    void start();
    void stop();
    // Blocks until the current instance has handled everything accepted so far.
    bool wait_until_drained(std::chrono::milliseconds timeout);

    [[nodiscard]] SignalProcessorStats stats() const;
    [[nodiscard]] StrategyControls& controls() noexcept { return *controls_; }
    [[nodiscard]] const StrategyConfig& config() const noexcept { return config_; }

    ProbeResult probe() override;
    RecoveryResult recover(const std::string& cause) override;
    void quarantine() override;

protected:
    void fill_status(ComponentStatus& status) const override;

private:
    [[nodiscard]] std::shared_ptr<SignalProcessor> processor() const;
    [[nodiscard]] std::shared_ptr<SignalProcessor> build(std::uint64_t generation, bool catch_up);
    void retire(const std::shared_ptr<SignalProcessor>& processor);

    ComponentContext context_;
    const StrategyConfig config_;
    const WorkerOptions worker_options_;
    std::shared_ptr<StrategyControls> controls_;
    std::shared_ptr<GenerationGate> gate_;
    StallDetector stall_;
    mutable std::mutex mutex_;
    std::shared_ptr<SignalProcessor> processor_;
    // Totals from retired instances, so cumulative figures survive a restart.
    SignalProcessorStats carried_{};
};

// Turns signals into orders. Recovery reconciles with the broker before it resubmits.
class ExecutionComponent final : public SupervisedComponent {
public:
    ExecutionComponent(const ComponentContext& context, IRequestManager& gateway, OrderExecutionConfig config);
    ~ExecutionComponent() override;

    void start();
    void stop();
    bool wait_until_drained(std::chrono::milliseconds timeout);

    [[nodiscard]] ExecutionStats stats() const;
    [[nodiscard]] FaultControl& fault() noexcept { return *fault_; }

    ProbeResult probe() override;
    RecoveryResult recover(const std::string& cause) override;
    void quarantine() override;

protected:
    void fill_status(ComponentStatus& status) const override;

private:
    [[nodiscard]] std::shared_ptr<OrderExecutionWorker> worker() const;
    [[nodiscard]] std::shared_ptr<OrderExecutionWorker> build(std::uint64_t generation, bool recover);

    ComponentContext context_;
    IRequestManager& gateway_;
    const OrderExecutionConfig config_;
    std::shared_ptr<FaultControl> fault_;
    std::shared_ptr<GenerationGate> gate_;
    StallDetector stall_;
    mutable std::mutex mutex_;
    std::shared_ptr<OrderExecutionWorker> worker_;
    ExecutionStats carried_{};
};

// Applies execution reports to the position book. The book outlives the monitor, and is
// idempotent, so recovery just reapplies the journaled reports.
class PositionComponent final : public SupervisedComponent {
public:
    PositionComponent(const ComponentContext& context, PositionBook& book, PositionMonitorConfig config);
    ~PositionComponent() override;

    void start();
    void stop();
    bool wait_until_drained(std::chrono::milliseconds timeout);

    [[nodiscard]] PositionMonitorStats stats() const;
    [[nodiscard]] FaultControl& fault() noexcept { return *fault_; }

    ProbeResult probe() override;
    RecoveryResult recover(const std::string& cause) override;
    void quarantine() override;

protected:
    void fill_status(ComponentStatus& status) const override;

private:
    [[nodiscard]] std::shared_ptr<PositionMonitor> monitor() const;
    [[nodiscard]] std::shared_ptr<PositionMonitor> build(std::uint64_t generation, bool recover);

    ComponentContext context_;
    PositionBook& book_;
    const PositionMonitorConfig config_;
    std::shared_ptr<FaultControl> fault_;
    std::shared_ptr<GenerationGate> gate_;
    StallDetector stall_;
    mutable std::mutex mutex_;
    std::shared_ptr<PositionMonitor> monitor_;
    PositionMonitorStats carried_{};
};

// A feed thread. A replacement continues the sequence numbers and prices of the one it
// replaces, the way a reconnecting feed handler resumes from its last sequence number.
class FeedComponent final : public SupervisedComponent {
public:
    FeedComponent(const ComponentContext& context, MarketDataProducerConfig config);
    ~FeedComponent() override;

    void start();
    void stop();
    void wait();

    [[nodiscard]] ProducerStats stats() const;
    // False once the feed has stopped for good, as opposed to having crashed.
    [[nodiscard]] bool finished_normally() const;
    [[nodiscard]] bool crashed() const;
    bool set_interval(std::chrono::microseconds interval);
    bool set_paused(bool paused);
    void inject_crash();

    ProbeResult probe() override;
    RecoveryResult recover(const std::string& cause) override;

private:
    [[nodiscard]] std::shared_ptr<MarketDataProducer> producer() const;
    [[nodiscard]] std::shared_ptr<MarketDataProducer> build(const MarketDataProducerConfig& config) const;

    ComponentContext context_;
    MarketDataProducerConfig config_;
    mutable std::mutex mutex_;
    std::shared_ptr<MarketDataProducer> producer_;
    std::uint64_t carried_published_{0};
    std::uint64_t carried_rejections_{0};
    bool stopping_{false};
};

}  // namespace trading
