#ifndef GUARDED_RELEASE_CONTROLLER_HPP
#define GUARDED_RELEASE_CONTROLLER_HPP
#include "deadline_certificate.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <string>
#include <vector>

namespace guarded_release {
// Snapshot deadlines are residual at now. old_units is the full installed
// schedule, including previously promised timer changes, not a constant floor.
struct Snapshot {
    int now=0, capacity=400, units=106, base=21, maximum=74;
    int step=5, delay=2, term=40, old_final_change=0;
    long double command_price=0;
    bool allow_release=true; // Predictive comparator disables the short release branch.
    std::vector<Envelope> envelopes;
    std::vector<PendingJob> pending;
    std::vector<int> old_units;
    std::vector<int> other_units;
};
struct Decision {
    bool certified=false, replace=false, release=false, no_feasible=false;
    int target=0, term=0, tail=0, horizon=0;
    std::size_t feasibility_calls=0;
    long double old_cost=0, proposed_cost=0, planning_us=0;
    std::string reason;
};
inline long double floor_service(const Snapshot&s,int u) {
    return std::floor(static_cast<long double>(s.capacity)*u/s.units);
}
inline int minimum_tail(const Snapshot&s) {
    for(long long value=s.base;value<=s.maximum;++value) {
        const int u=static_cast<int>(value);
        if(tail_condition(s.envelopes,floor_service(s,u)).feasible)return u;
    }
    return -1;
}
inline Decision guarded_decision(const Snapshot&s) {
    const auto started=std::chrono::steady_clock::now();
    Decision d;
    auto finish=[&]() { d.planning_us=std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-started).count();return d; };
    if(s.capacity<1||s.units<1||s.base<0||s.maximum<s.base||s.maximum>s.units||s.step<1||s.delay<0||s.term<1||s.old_final_change<0||!std::isfinite(s.command_price)||s.command_price<0)
        throw std::invalid_argument("Invalid controller parameters");
    if(s.delay>std::numeric_limits<int>::max()-std::max(s.step,s.term))throw std::invalid_argument("Plan time overflow");
    for(int u:s.old_units)if(u<s.base||u>s.maximum)throw std::invalid_argument("Old floor outside fixed interface bounds");
    for(int u:s.other_units)if(u<0||u>s.units)throw std::invalid_argument("Invalid other commitment");
    const int B=minimum_tail(s);d.tail=B;
    if(B<0){d.no_feasible=true;d.reason="unsustainable_contract";return finish();}
    const int H=std::max(required_horizon(std::max(s.old_final_change,s.delay+std::max(s.step,s.term)),s.envelopes,s.pending),static_cast<int>(s.other_units.size()));
    d.horizon=H;
    if(s.old_units.size()<static_cast<std::size_t>(H))throw std::invalid_argument("Complete old service calendar required");
    auto other=[&](int n){return n<static_cast<int>(s.other_units.size())?s.other_units[n]:s.units-s.maximum;};
    auto check=[&](int target,int L) {
        std::vector<long double> service(H);
        for(int n=0;n<H;++n){const int u=n<s.delay?s.old_units[n]:(n<s.delay+L?target:B);
            if(u>s.units-other(n))return false;
            service[n]=floor_service(s,u);}
        ++d.feasibility_calls;
        return plan_certificate(s.envelopes,s.pending,service,s.delay+L,floor_service(s,B)).feasible;
    };
    // Fixed protection term; the only short term is the next-observation gap.
    int target=s.base,L=s.step;
    if(s.allow_release && check(target,L))d.release=true;
    else {
        L=s.term;bool found=false;
        // A simple bounded scan makes resource/calendar conflicts explicit.
        // The class-aware certificate is O(KH+J); the full search is O(U(KH+J)).
        for(long long value=s.base;value<=s.maximum;++value){const int u=static_cast<int>(value);if(check(u,L)){target=u;found=true;break;}}
        if(!found){d.no_feasible=true;d.reason="retain_old_plan_no_feasible_replacement";return finish();}
    }
    d.certified=true;d.target=target;d.term=L;
    // Charge a common horizon, including every preloaded continuation slot.
    // The time-indexed admission ledger is a separate hard constraint above.
    bool identical=true;
    for(int n=0;n<H;++n){const int u=n<s.delay?s.old_units[n]:(n<s.delay+L?target:B);
        d.old_cost+=std::max(0,s.old_units[n]-s.base);
        d.proposed_cost+=std::max(0,u-s.base);
        identical=identical&&(u==s.old_units[n]);}
    // Retention is only permitted if the existing full plan is also certified
    // at this observation. An empirical contract violation must be visible.
    std::vector<long double> previous(H);
    bool old_resources=true;
    for(int n=0;n<H;++n){previous[n]=floor_service(s,s.old_units[n]);old_resources=old_resources&&(s.old_units[n]<=s.units-other(n));}
    ++d.feasibility_calls;
    const bool old_certified=old_resources&&plan_certificate(s.envelopes,s.pending,previous,s.old_final_change,previous.back()).feasible;
    if((identical&&old_certified) || (old_certified&&d.old_cost<=d.proposed_cost+s.command_price)) {
        d.reason="retain_certified_calendar";d.replace=false;
    } else {d.replace=true;d.reason=d.release?"guarded_release":"fixed_term_protection";}
    return finish();
}
}
#endif
