#include "trading/system.hpp"

#include <algorithm>
#include <ctime>
#include <random>
#include <set>
#include <stdexcept>
#include <utility>

namespace trading {
namespace {

using namespace std::chrono_literals;

void validate(const SystemConfig& config) {
    if (config.feeds.empty()) {
        throw std::invalid_argument("at least one market-data feed is required");
    }
    std::set<std::string> feed_names;
    std::set<std::string> symbols;
    for (const auto& feed : config.feeds) {
        if (feed.name.empty() || !feed_names.insert(feed.name).second) {
            throw std::invalid_argument("feed names must be unique and non-empty");
        }
        for (const auto& instrument : feed.instruments) {
            // One publisher per topic keeps every instrument's quotes in sequence order.
            if (!symbols.insert(instrument.symbol).second) {
                throw std::invalid_argument("instrument " + instrument.symbol +
                                            " is published by more than one feed");
            }
        }
    }

    std::set<std::string> strategy_names;
    for (const auto& strategy : config.strategies) {
        if (strategy.name.empty() || !strategy_names.insert(strategy.name).second) {
            throw std::invalid_argument("strategy names must be unique and non-empty");
        }
        if (!is_known_strategy_kind(strategy.kind)) {
            throw std::invalid_argument("strategy " + strategy.name + " has unknown kind " +
                                        strategy.kind);
        }
        for (const auto& symbol : strategy.symbols) {
            if (!symbols.contains(symbol)) {
                throw std::invalid_argument("strategy " + strategy.name +
                                            " subscribes to unknown instrument " + symbol);
            }
        }
    }
    if (config.gateways.count == 0U || config.gateways.count > 8U) {
        throw std::invalid_argument("between 1 and 8 gateways are supported");
    }
    if (config.supervision.enabled && config.replay) {
        throw std::invalid_argument("a replay system is not supervised");
    }
    if (config.journal.enabled && config.replay) {
        throw std::invalid_argument("a replay must not write a journal");
    }
}

template <typename Components, typename Action>
bool for_each_named(Components& components, std::string_view name, Action action) {
    bool matched = false;
    for (auto& component : components) {
        if (name == "*" || component->name() == name) {
            action(*component);
            matched = true;
        }
    }
    return matched;
}

std::string make_session_id() {
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::tm parts{};
#if defined(_WIN32)
    gmtime_s(&parts, &now);
#else
    gmtime_r(&now, &parts);
#endif
    char text[32];
    std::strftime(text, sizeof text, "%Y%m%dT%H%M%S", &parts);
    std::random_device entropy;
    char suffix[8];
    std::snprintf(suffix, sizeof suffix, "%03x", entropy() & 0xFFFU);
    return std::string(text) + "-" + suffix;
}

}  // namespace

SystemConfig SystemConfig::demo() {
    SystemConfig config;
    config.feeds = {
        MarketDataProducerConfig{
            .name = "equities",
            .instruments = {{"AAPL", 190.0, 1.0}, {"MSFT", 420.0, 1.0}, {"NVDA", 120.0, 1.0}},
            .seed = 7,
        },
        MarketDataProducerConfig{
            .name = "fx",
            .instruments = {{"EURUSD", 1.085, 1.0}, {"GBPUSD", 1.27, 1.0}},
            .seed = 11,
        },
    };
    config.strategies = {
        StrategyConfig{"trend-tech", "ema-cross", {"AAPL", "MSFT", "NVDA"}, 10.0},
        StrategyConfig{"trend-fx", "ema-cross", {"EURUSD", "GBPUSD"}, 10'000.0},
        StrategyConfig{"levels", "threshold", {"AAPL", "EURUSD"}, 100.0},
    };
    return config;
}

SystemConfig SystemConfig::for_replay(const SystemConfig& live, ReplayOptions options) {
    SystemConfig config = live;
    config.replay = std::move(options);
    config.journal.enabled = false;
    config.supervision.enabled = false;
    config.gateways = GatewaySettings{};
    // A replay's broker is its own and instant, and nothing about it may be throttled: the
    // point is to see the strategies' decisions, not the live limits.
    config.api_manager.rate_limit = RateLimitConfig{1'000'000, 1'000ms};
    config.api_manager.max_outstanding_requests = 65'536;
    config.mock_api = MockApiConfig{};
    config.mock_api.latency = 0ms;
    config.execution.worker.mailbox_capacity = 65'536;
    config.position_monitor.worker.mailbox_capacity = 65'536;
    config.record_activity = true;
    config.session = "replay-" + (live.session.empty() ? std::string("session") : live.session);
    return config;
}

std::optional<SystemConfig> SystemConfig::from_manifest(const std::filesystem::path& session_directory,
                                                        const SystemConfig& base) {
    const auto manifest = read_manifest(session_directory);
    if (!manifest || manifest->feeds.empty()) {
        return std::nullopt;
    }
    SystemConfig config = base;
    config.feeds.clear();
    config.strategies.clear();
    std::uint64_t seed = 1;
    for (const auto& feed : manifest->feeds) {
        MarketDataProducerConfig producer;
        producer.name = feed.name;
        producer.seed = seed++;
        for (const auto& [symbol, price] : feed.instruments) {
            producer.instruments.push_back(InstrumentConfig{symbol, price, 1.0});
        }
        config.feeds.push_back(std::move(producer));
    }
    for (const auto& strategy : manifest->strategies) {
        config.strategies.push_back(StrategyConfig{strategy.name, strategy.kind, strategy.symbols, strategy.quantity});
    }
    config.session = manifest->session;
    return config;
}

TradingSystem::TradingSystem(SystemConfig config)
    : config_(std::move(config)), epoch_(MonotonicClock::now()) {
    validate(config_);
    const bool replaying = config_.replay.has_value();
    const bool playback = replaying && config_.replay->mode == ReplayMode::playback;
    session_ = config_.session.empty() ? make_session_id() : config_.session;

    if (config_.journal.enabled) {
        JournalConfig journal;
        journal.directory = config_.journal.root / session_;
        journal.session = session_;
        journal.segment_bytes = config_.journal.segment_bytes;
        journal.mailbox_capacity = config_.journal.mailbox_capacity;
        journal.flush_interval = config_.journal.flush_interval;
        journal.fsync_interval = config_.journal.fsync_interval;
        journal.full_fsync = config_.journal.full_fsync;
        journal_ = std::make_unique<JournalWriter>(bus_, journal);
        write_manifest(journal.directory, manifest());
    }

    mock_api_ = std::make_shared<MockTradingApi>(config_.mock_api);
    std::vector<GatewayPool::Member> members;
    for (std::size_t index = 0; index < config_.gateways.count; ++index) {
        auto fault = std::make_shared<FaultableApiClient>(mock_api_);
        members.push_back(GatewayPool::Member{
            "gateway-" + std::to_string(index + 1U),
            std::make_shared<ApiRequestManager>(fault, config_.api_manager),
            fault,
        });
    }
    gateway_ = std::make_unique<GatewayPool>(std::move(members), config_.gateways.pool, 42);

    const ComponentContext context{bus_, graveyard_, config_.supervision.config, journal_.get()};
    position_monitor_ = std::make_unique<PositionComponent>(context, positions_, config_.position_monitor);

    // A playback shows what was recorded, signals and fills included, so nothing here may
    // recompute them: no strategies and no order execution, or every signal would be acted
    // on a second time.
    if (!playback) {
        auto execution = config_.execution;
        execution.session = session_;
        execution_ = std::make_unique<ExecutionComponent>(context, *gateway_, std::move(execution));
        for (const auto& strategy : config_.strategies) {
            strategies_.push_back(std::make_unique<StrategyComponent>(context, strategy, config_.strategy_worker));
        }
    }
    if (config_.record_activity) {
        activity_ = std::make_unique<ActivityRecorder>(bus_, config_.activity, epoch_);
    }

    if (replaying) {
        replay_feed_ = std::make_unique<ReplayFeed>(bus_, *config_.replay, [this] { wait_for_downstream(); });
        if (config_.replay->mode == ReplayMode::rederive) {
            const auto bounds = JournalReader::bounds(config_.replay->directory);
            verifier_ = std::make_unique<ReplayVerifier>(bus_, config_.replay->directory, replay_feed_->first_sequence(),
                                                         replay_feed_->last_sequence(),
                                                         replay_feed_->first_sequence() == bounds.first);
        }
    } else {
        for (const auto& feed : config_.feeds) {
            feeds_.push_back(std::make_unique<FeedComponent>(context, feed));
        }
    }

    if (config_.supervision.enabled) {
        supervisor_ = std::make_unique<Supervisor>(bus_, graveyard_, config_.supervision.config, epoch_);
        for (auto* component : components()) {
            supervisor_->add(*component);
        }
    }
}

TradingSystem::~TradingSystem() {
    stop();
}

std::vector<SupervisedComponent*> TradingSystem::components() const {
    std::vector<SupervisedComponent*> all;
    for (const auto& feed : feeds_) {
        all.push_back(feed.get());
    }
    for (const auto& strategy : strategies_) {
        all.push_back(strategy.get());
    }
    if (execution_) {
        all.push_back(execution_.get());
    }
    all.push_back(position_monitor_.get());
    return all;
}

SessionManifest TradingSystem::manifest() const {
    SessionManifest manifest;
    manifest.session = session_;
    for (const auto& feed : config_.feeds) {
        ManifestFeed entry;
        entry.name = feed.name;
        for (const auto& instrument : feed.instruments) {
            entry.instruments.emplace_back(instrument.symbol, instrument.reference_price);
        }
        manifest.feeds.push_back(std::move(entry));
    }
    for (const auto& strategy : config_.strategies) {
        manifest.strategies.push_back(ManifestStrategy{strategy.name, strategy.kind, strategy.symbols, strategy.quantity});
    }
    return manifest;
}

std::filesystem::path TradingSystem::journal_directory() const {
    if (journal_) {
        return journal_->directory();
    }
    return config_.replay ? config_.replay->directory : std::filesystem::path{};
}

void TradingSystem::set_escalation(std::function<void(const std::string&)> escalate) {
    if (supervisor_) {
        supervisor_->set_escalation(std::move(escalate));
    }
}

void TradingSystem::wait_for_downstream() {
    // Strategies first: they publish signals before their handler returns, so once they
    // are drained everything they will emit is already queued for the stages after them.
    constexpr auto limit = 10s;
    for (auto& strategy : strategies_) {
        strategy->wait_until_drained(limit);
    }
    if (verifier_) {
        verifier_->wait_until_drained(limit);
    }
    if (execution_) {
        execution_->wait_until_drained(limit);
    }
}

void TradingSystem::start() {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_.load() != State::created) {
        throw std::logic_error("a trading system can only be started once");
    }
    if (journal_) {
        journal_->start();
    }
    if (activity_) {
        activity_->start();
    }
    position_monitor_->start();
    if (execution_) {
        execution_->start();
    }
    for (auto& strategy : strategies_) {
        strategy->start();
    }
    if (verifier_) {
        verifier_->start();
    }
    for (auto& feed : feeds_) {
        feed->start();
    }
    if (replay_feed_) {
        replay_feed_->start();
    }
    if (supervisor_) {
        supervisor_->start();
    }
    state_.store(State::running);
}

void TradingSystem::stop() {
    std::lock_guard lock(lifecycle_mutex_);
    if (state_.exchange(State::stopped) == State::stopped) {
        return;
    }
    // The supervisor goes first, or it would "heal" components that are being stopped.
    if (supervisor_) {
        supervisor_->stop();
    }
    // Wake anything an injected fault is holding, so that shutdown cannot wait for it.
    for (auto& strategy : strategies_) {
        strategy->controls().fault.release_all();
    }
    if (execution_) {
        execution_->fault().release_all();
    }
    position_monitor_->fault().release_all();

    if (replay_feed_) {
        replay_feed_->stop();
        replay_feed_->wait();
    }
    for (auto& feed : feeds_) {
        feed->stop();
    }
    for (auto& feed : feeds_) {
        feed->wait();
    }
    // Draining queued market data should not be slowed by a simulated slow consumer.
    for (auto& strategy : strategies_) {
        strategy->controls().work_us.store(0, std::memory_order_relaxed);
    }
    for (auto& strategy : strategies_) {
        strategy->stop();
    }
    if (verifier_) {
        verifier_->stop();
    }
    if (execution_) {
        execution_->stop();
    }
    gateway_->shutdown(ApiShutdownMode::drain, config_.drain_timeout);
    position_monitor_->stop();
    if (activity_) {
        activity_->stop();
    }
    if (journal_) {
        journal_->stop();
    }
    // Abandoned threads end by themselves once released; give them a moment so that their
    // objects are freed rather than leaked.
    graveyard_.wait_empty(2'000ms);
}

bool TradingSystem::feeds_finished() const {
    if (replay_feed_) {
        return replay_feed_->finished();
    }
    return std::all_of(feeds_.begin(), feeds_.end(), [this](const auto& feed) {
        if (feed->finished_normally()) {
            return true;
        }
        // A crashed feed is about to be replaced, unless nothing is supervising it.
        return (feed->crashed() && !config_.supervision.enabled) ||
               feed->status().state == ComponentState::quarantined;
    });
}

SystemSnapshot TradingSystem::snapshot() const {
    SystemSnapshot snapshot;
    snapshot.uptime_s = std::chrono::duration<double>(MonotonicClock::now() - epoch_).count();
    snapshot.running = state_.load() == State::running;
    snapshot.mode = config_.replay ? "replay" : "live";
    snapshot.session = session_;
    snapshot.supervised = config_.supervision.enabled;

    std::optional<ReplayProgress> progress;
    if (replay_feed_) {
        progress = replay_feed_->progress();
        // The virtual feeds of a replay: each configured feed, with the quotes the replay
        // has published on its topics so far.
        for (const auto& feed : config_.feeds) {
            ProducerStats stats;
            stats.name = feed.name;
            for (const auto& instrument : feed.instruments) {
                stats.symbols.push_back(instrument.symbol);
                const auto found = progress->by_topic.find(instrument.symbol);
                if (found != progress->by_topic.end()) {
                    stats.events_published += found->second;
                }
            }
            stats.paused = progress->state == ReplayState::paused;
            stats.finished = progress->state == ReplayState::finished || progress->state == ReplayState::failed;
            snapshot.feeds.push_back(std::move(stats));
        }
    }
    for (const auto& feed : feeds_) {
        snapshot.feeds.push_back(feed->stats());
    }
    for (const auto& strategy : strategies_) {
        snapshot.strategies.push_back(strategy->stats());
        snapshot.strategy_work.push_back(
            std::chrono::microseconds(strategy->controls().work_us.load(std::memory_order_relaxed)));
    }
    if (execution_) {
        snapshot.execution = execution_->stats();
    }
    snapshot.api = gateway_->stats();
    snapshot.mock_api_calls = mock_api_->invocation_count();
    snapshot.mock_api_latency = mock_api_->latency();
    snapshot.mock_api_429_every = mock_api_->respond_429_every();
    snapshot.position_monitor = position_monitor_->stats();
    snapshot.positions = positions_.snapshot();
    snapshot.bus = bus_.stats();
    snapshot.routes = bus_.routes();
    if (activity_) {
        snapshot.activity = activity_->snapshot();
    }

    for (const auto* component : components()) {
        snapshot.components.push_back(component->status());
    }
    if (supervisor_) {
        snapshot.supervision_log = supervisor_->log();
        snapshot.supervisor = supervisor_->stats();
    }
    if (journal_) {
        snapshot.journal = journal_->stats();
    }
    snapshot.gateways = gateway_->gateways();
    snapshot.gateway_pool = gateway_->pool_stats();
    if (progress) {
        ReplaySnapshot replay;
        replay.progress = std::move(*progress);
        if (verifier_) {
            // Absences are final once the feed has finished: it waits for downstream first.
            replay.verification = verifier_->report(replay_feed_->finished() && !replay_feed_->seeked());
        }
        snapshot.replay = std::move(replay);
    }
    return snapshot;
}

bool TradingSystem::set_feed_interval(std::string_view feed, std::chrono::microseconds interval) {
    return for_each_named(feeds_, feed, [interval](FeedComponent& component) { component.set_interval(interval); });
}

bool TradingSystem::set_feed_paused(std::string_view feed, bool paused) {
    return for_each_named(feeds_, feed, [paused](FeedComponent& component) { component.set_paused(paused); });
}

bool TradingSystem::set_strategy_work(std::string_view strategy, std::chrono::microseconds work) {
    if (work < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("simulated work must not be negative");
    }
    return for_each_named(strategies_, strategy, [work](StrategyComponent& component) {
        component.controls().work_us.store(work.count(), std::memory_order_relaxed);
    });
}

void TradingSystem::set_api_latency(std::chrono::milliseconds latency) {
    mock_api_->set_latency(latency);
}

void TradingSystem::set_api_429_every(std::size_t every_nth) {
    mock_api_->set_respond_429_every(every_nth);
}

bool TradingSystem::inject_fault(std::string_view component, Fault fault, std::chrono::milliseconds hang_for) {
    const auto arm = [&](FaultControl& control) {
        switch (fault) {
            case Fault::crash:
                control.arm_crash();
                break;
            case Fault::hang:
                control.arm_hang(hang_for);
                break;
            case Fault::poison:
                // Every event fails until the supervisor replaces the instance.
                control.arm_poison(1'000'000);
                break;
        }
    };
    for (auto& strategy : strategies_) {
        if (strategy->name() == component) {
            arm(strategy->controls().fault);
            return true;
        }
    }
    if (execution_ && component == execution_->name()) {
        arm(execution_->fault());
        return true;
    }
    if (component == position_monitor_->name()) {
        arm(position_monitor_->fault());
        return true;
    }
    for (auto& feed : feeds_) {
        if (feed->name() == component) {
            if (fault != Fault::crash) {
                return false;
            }
            feed->inject_crash();
            return true;
        }
    }
    return false;
}

bool TradingSystem::set_gateway_down(std::size_t index, bool down) {
    return gateway_->set_down(index, down);
}

bool TradingSystem::replay_set_paused(bool paused) {
    if (!replay_feed_) {
        return false;
    }
    replay_feed_->set_paused(paused);
    return true;
}

bool TradingSystem::replay_set_speed(double speed) {
    if (!replay_feed_ || speed <= 0.0) {
        return false;
    }
    replay_feed_->set_speed(speed);
    return true;
}

bool TradingSystem::replay_set_pacing(ReplayPacing pacing) {
    if (!replay_feed_) {
        return false;
    }
    replay_feed_->set_pacing(pacing);
    return true;
}

bool TradingSystem::replay_seek(double fraction) {
    if (!replay_feed_) {
        return false;
    }
    fraction = std::clamp(fraction, 0.0, 1.0);
    const auto first = replay_feed_->first_sequence();
    const auto last = replay_feed_->last_sequence();
    replay_feed_->seek(first + static_cast<std::uint64_t>(fraction * static_cast<double>(last - first)));
    if (verifier_) {
        verifier_->mark_incomparable("the replay was moved, so the strategies did not see the whole recording");
    }
    return true;
}

}  // namespace trading
