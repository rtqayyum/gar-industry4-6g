#ifndef GUARDED_RELEASE_PLAN_EXECUTOR_HPP
#define GUARDED_RELEASE_PLAN_EXECUTOR_HPP

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>

namespace guarded_release {

// Execute versioned, piecewise-constant service-floor plans at slot boundaries.
// This class executes caller-approved plans; it neither certifies deadlines
// nor admits capacity. The guarded adapter approves only resource-admitted and
// deadline-certified plans. Empirical baseline adapters may approve resource-
// admitted plans without deadline certification and must record that distinction.
// The Boolean below is an installation gate, not a proof. A floor is an integer
// accounting unit.
//
// At each slot t: advance(t), observe/plan, optionally propose(t,...), then
// serve using current_units(). A zero-delay accepted proposal installs at t.
// A positive-delay proposal leaves the COMPLETE old plan in force until its
// installation, including any old fallback that occurs strictly before then.
// Installation atomically replaces the old plan; a coincident old fallback is
// superseded. The new target serves [installation, installation+term), and its
// tail serves forever afterwards. Nothing silently releases a tail at expiry.
//
// units_at(t) and committed_at(t) are the exact projected floor calendar,
// including an accepted pending replacement. max_future_units(t) is a distinct
// undated standby-right maximum; it is not a calendar entry or monetary cost.
// All projections require t >= now(). A later accepted proposal can change
// that projection; a broker must approve any resulting calendar change.
class TimedPlanExecutor {
public:
    using Time = std::int64_t;

    struct Counters {
        std::uint64_t plan_commands = 0;           // Valid proposal attempts.
        std::uint64_t accepted_plan_commands = 0;
        std::uint64_t rejected_commands = 0;       // Unapproved or pending.
        std::uint64_t timer_updates = 0;           // Atomic installations.
        std::uint64_t actual_floor_changes = 0;   // Distinct applied values.
        std::uint64_t backup_activations = 0;      // Includes unchanged tails.
        std::uint64_t renewals = 0;               // Accepted target=current.
        std::uint64_t superseded_before_expiry = 0;
    };

    explicit TimedPlanExecutor(int initial_units = 21, Time start_time = 0)
        : clock_(start_time), current_units_(initial_units) {
        if (initial_units < 0 || start_time < 0) {
            throw std::invalid_argument("Initial units and time must be nonnegative");
        }
        active_.target = initial_units;
        active_.tail = initial_units;
        active_.installed_at = start_time;
    }

    // Advance even if observations were missed. All due events are processed
    // in chronological order, preserving their counters and the service plan.
    // Repeated advance(t) is idempotent; moving time backwards is an error.
    void advance(Time t) {
        require_future(t);
        while (true) {
            const bool install_due = pending_ && pending_->installed_at <= t;
            const bool backup_due = active_.timer_armed && active_.expiry <= t;
            if (!install_due && !backup_due) {
                break;
            }
            // <= deliberately gives a replacement priority over a coincident
            // old backup. There is only one installed floor for this event.
            if (install_due && (!backup_due ||
                                pending_->installed_at <= active_.expiry)) {
                const Time when = pending_->installed_at;
                if (active_.timer_armed && when < active_.expiry) {
                    ++counts_.superseded_before_expiry;
                }
                active_ = *pending_;
                pending_.reset();
                ++counts_.timer_updates;
                apply_units(active_.target);
            } else {
                active_.timer_armed = false;
                ++counts_.backup_activations;
                apply_units(active_.tail);
            }
        }
        clock_ = t;
    }

    // Call only after advance(now), or at the constructor's initial boundary.
    // Rejection preserves both the installed plan and any pending replacement.
    // Invalid arguments throw before modifying counters or state. Ordinary
    // rejection returns false and records the attempted command.
    bool propose(Time now, Time delay, int target, Time term, int tail,
                 bool approved_for_installation) {
        if (now != clock_) {
            throw std::logic_error("Call advance(now) before proposing a plan");
        }
        if (delay < 0 || term <= 0 || target < 0 || tail < 0) {
            throw std::invalid_argument("Invalid plan delay, term, or floor");
        }
        const Time install = checked_add(now, delay);
        const Time expiry = checked_add(install, term);
        ++counts_.plan_commands;
        if (!approved_for_installation || pending_) {
            ++counts_.rejected_commands;
            return false;
        }
        ++counts_.accepted_plan_commands;
        if (target == current_units_) {
            ++counts_.renewals;
        }
        Plan next;
        next.id = next_id_++;
        next.installed_at = install;
        next.expiry = expiry;
        next.target = target;
        next.tail = tail;
        next.timer_armed = true;
        pending_ = next;
        if (delay == 0) {
            advance(now);
        }
        return true;
    }

    int units_at(Time t) const {
        require_future(t);
        if (pending_ && t >= pending_->installed_at) {
            return plan_units_at(*pending_, t);
        }
        return plan_units_at(active_, t);
    }

    // Exact time-indexed committed service, not the undated option maximum.
    int committed_at(Time t) const { return units_at(t); }

    int max_future_units(Time t) const {
        require_future(t);
        int maximum = units_at(t);
        if (pending_ && t >= pending_->installed_at) {
            return std::max(maximum, pending_->tail);
        }
        if (pending_) {
            // Old fallback matters only if it takes effect before replacement.
            if (active_.has_expiry() && active_.expiry < pending_->installed_at) {
                maximum = std::max(maximum, active_.tail);
            }
            maximum = std::max(maximum, pending_->target);
            return std::max(maximum, pending_->tail);
        }
        return std::max(maximum, active_.tail);
    }

    int current_units() const { return current_units_; }
    Time now() const { return clock_; }
    bool has_pending() const { return pending_.has_value(); }
    std::optional<Time> pending_install_time() const {
        return pending_ ? std::optional<Time>(pending_->installed_at) : std::nullopt;
    }
    // The active timer is absent after it fires; its tail is still installed.
    std::optional<Time> active_expiry() const {
        return active_.timer_armed ? std::optional<Time>(active_.expiry) : std::nullopt;
    }
    std::uint64_t active_plan_id() const { return active_.id; }
    std::optional<std::uint64_t> pending_plan_id() const {
        return pending_ ? std::optional<std::uint64_t>(pending_->id) : std::nullopt;
    }
    const Counters& counters() const { return counts_; }

private:
    struct Plan {
        std::uint64_t id = 0;
        Time installed_at = 0;
        Time expiry = 0;
        int target = 0;
        int tail = 0;
        bool timer_armed = false;
        // Plan 0 is the initial perpetual floor, without a timed transition.
        bool has_expiry() const { return id != 0; }
    };

    static int plan_units_at(const Plan& plan, Time t) {
        return plan.has_expiry() && t >= plan.expiry ? plan.tail : plan.target;
    }
    static Time checked_add(Time lhs, Time rhs) {
        if (rhs > std::numeric_limits<Time>::max() - lhs) {
            throw std::overflow_error("Plan timestamp overflow");
        }
        return lhs + rhs;
    }
    void require_future(Time t) const {
        if (t < clock_) {
            throw std::invalid_argument("Executor time cannot move backwards");
        }
    }
    void apply_units(int units) {
        if (units != current_units_) {
            ++counts_.actual_floor_changes;
            current_units_ = units;
        }
    }

    Time clock_;
    int current_units_;
    Plan active_;
    std::optional<Plan> pending_;
    Counters counts_;
    std::uint64_t next_id_ = 1;
};

} // namespace guarded_release

#endif // GUARDED_RELEASE_PLAN_EXECUTOR_HPP
