#ifndef GUARDED_RELEASE_DEADLINE_CERTIFICATE_HPP
#define GUARDED_RELEASE_DEADLINE_CERTIFICATE_HPP

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <deque>
#include <limits>
#include <stdexcept>
#include <vector>

namespace guarded_release {

// Byte-work envelope over n consecutive unobserved release slots:
// alpha(0)=0 and alpha(n)=sigma+rho*n for n>=1.
// The caller supplies envelopes sorted by nondecreasing min_deadline.
struct Envelope {
    int min_deadline;
    long double sigma;
    long double rho;
};

struct PendingJob {
    int deadline;             // Residual deadline boundary; must be positive.
    long double remaining;   // Residual work already released at relative zero.
};

struct CheckResult {
    bool feasible = true;
    int a = -1;
    int b = -1;
    long double demand = 0;
    long double service = 0;
    const char* reason = "feasible";
    std::size_t checks = 0;  // Prefix ends plus nonempty active-prefix ranges.
};

inline void validate_envelopes(const std::vector<Envelope>& envelopes) {
    int prior = 0;
    for (const auto& e : envelopes) {
        if (e.min_deadline < 1 || e.min_deadline < prior ||
            !std::isfinite(e.sigma) || !std::isfinite(e.rho) ||
            e.sigma < 0 || e.rho < 0) {
            throw std::invalid_argument("Envelopes require sorted positive deadlines and finite nonnegative work");
        }
        prior = e.min_deadline;
    }
}

inline void validate_pending(const std::vector<PendingJob>& pending) {
    for (const auto& p : pending) {
        if (p.deadline < 1 || !std::isfinite(p.remaining) || p.remaining < 0) {
            throw std::invalid_argument("Pending jobs require positive residual deadlines and finite nonnegative work");
        }
    }
}

inline long double alpha(const Envelope& envelope, int slots) {
    return slots <= 0 ? 0.L : envelope.sigma + envelope.rho * slots;
}

inline long double interval_demand(const std::vector<Envelope>& envelopes,
                                   const std::vector<PendingJob>& pending,
                                   int a, int b) {
    if (a < 0 || b <= a) throw std::invalid_argument("Interval requires 0 <= a < b");
    long double demand = 0;
    if (a == 0) {
        for (const auto& p : pending) if (p.deadline <= b) demand += p.remaining;
    }
    for (const auto& e : envelopes) {
        const int count = std::max(0, b - e.min_deadline - std::max(a, 1) + 1);
        demand += alpha(e, count);
    }
    return demand;
}

// Tests every integer interval 0<=a<b<=H in O((K+1)H + J) arithmetic operations,
// where K is the envelope count and J is the number of pending jobs.
// service[n] is guaranteed critical work in [n,n+1). It is supplied verbatim:
// this function does not erase an old timer, pending command, or continuation.
// Arithmetic uses long double and no positive acceptance tolerance.
inline CheckResult finite_certificate(const std::vector<Envelope>& envelopes,
                                      const std::vector<PendingJob>& pending,
                                      const std::vector<long double>& service) {
    validate_envelopes(envelopes);
    validate_pending(pending);
    if (service.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
        throw std::invalid_argument("Horizon exceeds integer range");
    const int H = static_cast<int>(service.size());
    std::vector<long double> prefix(H + 1, 0), due(H + 1, 0);
    for (int n = 0; n < H; ++n) {
        if (!std::isfinite(service[n]) || service[n] < 0)
            throw std::invalid_argument("Service must be finite and nonnegative");
        prefix[n + 1] = prefix[n] + service[n];
    }
    for (const auto& p : pending) if (p.deadline <= H) due[p.deadline] += p.remaining;
    CheckResult result;
    long double queued = 0;
    for (int b = 1; b <= H; ++b) {
        queued += due[b];
        long double demand = queued;
        for (const auto& e : envelopes) demand += alpha(e, std::max(0, b - e.min_deadline));
        ++result.checks;
        if (demand > prefix[b]) {
            result.feasible = false; result.a = 0; result.b = b;
            result.demand = demand; result.service = prefix[b];
            result.reason = "prefix_demand";
            return result;
        }
    }

    // For fixed b, the positive starts partition into ranges having exactly
    // one active deadline prefix. A monotone deque maintains max(P(a)-R*a)
    // over each moving range; each a enters and leaves once per prefix.
    long double rate = 0, intercept = 0;
    struct Entry { int a; long double value; };
    for (std::size_t k = 0; k < envelopes.size(); ++k) {
        rate += envelopes[k].rho;
        intercept += envelopes[k].sigma - envelopes[k].rho * envelopes[k].min_deadline;
        std::deque<Entry> maximum;
        for (int b = 1; b <= H; ++b) {
            const int upper = b - envelopes[k].min_deadline;
            const int lower = k + 1 < envelopes.size()
                ? std::max(1, b - envelopes[k + 1].min_deadline + 1) : 1;
            if (upper >= 1) {
                const Entry next{upper, prefix[upper] - rate * upper};
                while (!maximum.empty() && maximum.back().value <= next.value) maximum.pop_back();
                maximum.push_back(next);
            }
            while (!maximum.empty() && maximum.front().a < lower) maximum.pop_front();
            if (lower > upper || maximum.empty()) continue;
            ++result.checks;
            const Entry worst = maximum.front();
            const long double required = intercept + rate * (b + 1.L) + worst.value;
            if (required > prefix[b]) {
                result.feasible = false; result.a = worst.a; result.b = b;
                result.demand = interval_demand(envelopes, pending, worst.a, b);
                result.service = prefix[b] - prefix[worst.a];
                result.reason = "future_interval_demand";
                return result;
            }
        }
    }
    return result;
}

// Sufficient for all intervals contained wholly in a constant-service tail.
// At every distinct deadline breakpoint m, test demand for interval length m.
inline CheckResult tail_condition(const std::vector<Envelope>& envelopes, long double tail_service) {
    validate_envelopes(envelopes);
    if (!std::isfinite(tail_service) || tail_service < 0)
        throw std::invalid_argument("Tail service must be finite and nonnegative");
    CheckResult result;
    long double total_rate = 0;
    for (const auto& e : envelopes) total_rate += e.rho;
    ++result.checks;
    if (total_rate > tail_service) {
        result.feasible = false; result.demand = total_rate; result.service = tail_service;
        result.reason = "tail_rate";
        return result;
    }
    long double rate = 0, intercept = 0;
    for (std::size_t k = 0; k < envelopes.size(); ++k) {
        rate += envelopes[k].rho;
        intercept += envelopes[k].sigma - envelopes[k].rho * envelopes[k].min_deadline;
        if (k + 1 < envelopes.size() && envelopes[k + 1].min_deadline == envelopes[k].min_deadline) continue;
        const int m = envelopes[k].min_deadline;
        const long double demand = intercept + rate * (m + 1.L);
        const long double available = tail_service * m;
        ++result.checks;
        if (demand > available) {
            result.feasible = false; result.a = 0; result.b = m;
            result.demand = demand; result.service = available;
            result.reason = "tail_deadline_breakpoint";
            return result;
        }
    }
    return result;
}

inline int required_horizon(int final_change, const std::vector<Envelope>& envelopes,
                            const std::vector<PendingJob>& pending) {
    validate_envelopes(envelopes);
    validate_pending(pending);
    if (final_change < 0) throw std::invalid_argument("Final change must be nonnegative");
    const int max_deadline = envelopes.empty() ? 0 : envelopes.back().min_deadline;
    if (final_change > std::numeric_limits<int>::max() - max_deadline - 1)
        throw std::invalid_argument("Required horizon exceeds integer range");
    int H = final_change + max_deadline + 1;
    for (const auto& p : pending) H = std::max(H, p.deadline);
    return H;
}

// Adds the explicitly checked prerequisites for an indefinitely continuing
// constant tail. This is conditional on the arrival envelopes and actual
// delivered service bounds remaining valid, and on sufficient queue storage.
inline CheckResult plan_certificate(const std::vector<Envelope>& envelopes,
                                    const std::vector<PendingJob>& pending,
                                    const std::vector<long double>& service,
                                    int final_change, long double tail_service) {
    const int required = required_horizon(final_change, envelopes, pending);
    if (service.size() < static_cast<std::size_t>(required))
        return {false, -1, -1, static_cast<long double>(required),
                static_cast<long double>(service.size()), "horizon_too_short", 0};
    CheckResult tail = tail_condition(envelopes, tail_service);
    if (!tail.feasible) return tail;
    for (std::size_t n = final_change; n < service.size(); ++n) {
        if (service[n] != tail_service)
            return {false, static_cast<int>(n), static_cast<int>(n + 1), tail_service,
                    service[n], "tail_not_constant", 0};
    }
    return finite_certificate(envelopes, pending, service);
}

} // namespace guarded_release
#endif
