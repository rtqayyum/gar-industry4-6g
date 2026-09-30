#ifndef GUARDED_RELEASE_BACKGROUND_ADMISSION_GATE_HPP
#define GUARDED_RELEASE_BACKGROUND_ADMISSION_GATE_HPP
#include "deadline_certificate.hpp"
#include <array>
#include <cstdint>
#include <string>

namespace guarded_release {

// Causal, fixed-width history: after advance(t) and this slot's observe calls,
// totals cover [max(0,t-99),t]. The denominator is always100; negative times
// have zero observations. The class never receives future packet records.
class OriginalArrivalWindow {
public:
    static constexpr int width=100,types=7;
    using Amount=std::int64_t;
private:
    std::array<std::array<Amount,types>,width> ring_{};
    std::array<Amount,types> totals_{};
    int now_=-1;
public:
    void advance(int now) {
        if(now<0||static_cast<std::int64_t>(now)!=static_cast<std::int64_t>(now_)+1)
            throw std::invalid_argument("Arrival window requires consecutive observed slots");
        auto& expired=ring_[now%width];
        for(int k=0;k<types;++k){totals_[k]-=expired[k];expired[k]=0;}
        now_=now;
    }
    void observe(int type,Amount bytes) {
        if(now_<0||type<0||type>=types||bytes<0)
            throw std::invalid_argument("Invalid observed original arrival");
        if(bytes>std::numeric_limits<Amount>::max()-totals_[type])
            throw std::overflow_error("Arrival history byte overflow");
        ring_[now_%width][type]+=bytes;totals_[type]+=bytes;
    }
    int now()const{return now_;}
    const std::array<Amount,types>& totals()const{return totals_;}
    std::array<long double,types> rates(long double eta)const {
        if(!std::isfinite(eta)||eta<0)throw std::invalid_argument("Invalid forecast multiplier");
        std::array<long double,types> result{};
        for(int k=0;k<types;++k){result[k]=eta*totals_[k]/width;
            if(!std::isfinite(result[k]))throw std::overflow_error("Forecast rate overflow");}
        return result;
    }
};

struct HeadroomInput {
    int capacity=0,request_offset=0,request_term=0;
    std::int64_t request_bytes_per_slot=0;
    std::vector<PendingJob> pending;
    // Sorted source-type relative deadlines, sigma=0, observed-rate forecast.
    // These are predictions, never described as enforced arrival contracts.
    std::vector<Envelope> forecast;
    // Sum each OLD lease's individual integer byte quota, not floor of the
    // aggregate resource-unit sum. All future slots through H are supplied.
    std::vector<std::int64_t> booked_service;
};
struct HeadroomDecision {
    bool pass=false;
    int horizon=0;
    std::size_t interval_checks=0;
    int failing_start=-1,failing_end=-1;
    long double demand=0,available=0;
    std::string reason;
};

inline HeadroomDecision deadline_headroom_gate(const HeadroomInput& s) {
    if(s.capacity<1||s.request_offset<0||s.request_term<1||s.request_bytes_per_slot<0||
       static_cast<std::int64_t>(s.request_offset)+s.request_term>std::numeric_limits<int>::max())
        throw std::invalid_argument("Invalid headroom request");
    validate_envelopes(s.forecast);validate_pending(s.pending);
    for(const auto&e:s.forecast)if(e.sigma!=0)
        throw std::invalid_argument("Headroom forecast has zero burst intercept");
    const int end=s.request_offset+s.request_term;
    const int min_horizon=required_horizon(end,s.forecast,s.pending);
    if(s.booked_service.size()<static_cast<std::size_t>(min_horizon)||
       s.booked_service.size()>static_cast<std::size_t>(std::numeric_limits<int>::max()))
        throw std::invalid_argument("Incomplete headroom service horizon");
    HeadroomDecision d;d.horizon=static_cast<int>(s.booked_service.size());
    std::vector<long double> service(d.horizon);
    for(int n=0;n<d.horizon;++n){
        const auto old=s.booked_service[n];
        if(old<0||old>s.capacity)throw std::invalid_argument("Invalid existing tenant byte service");
        const auto request=n>=s.request_offset&&n<end?s.request_bytes_per_slot:0;
        if(request>s.capacity-old){d.reason="tenant_service_exceeds_capacity";d.failing_start=n;d.failing_end=n+1;return d;}
        service[n]=s.capacity-old-request;
    }
    const auto check=finite_certificate(s.forecast,s.pending,service);
    d.pass=check.feasible;d.interval_checks=check.checks;d.failing_start=check.a;d.failing_end=check.b;
    d.demand=check.demand;d.available=check.service;
    d.reason=check.feasible?"forecast_headroom_pass":"forecast_interval_pressure";
    return d;
}
// Passing this finite aggregate forecast test is not a deadline guarantee for
// actual future arrivals or the weighted/non-global-EDF dispatchers. The
// original critical-flow enforced certificate remains an independent check.
}
#endif
