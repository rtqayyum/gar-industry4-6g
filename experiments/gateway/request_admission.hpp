#ifndef GUARDED_RELEASE_REQUEST_ADMISSION_HPP
#define GUARDED_RELEASE_REQUEST_ADMISSION_HPP
#include "controller.hpp"
namespace guarded_release {
struct RequestPlan {
    bool feasible=false;
    int target=0,term=0,tail=0,capacity_cap=0,horizon=0;
    std::size_t certificate_calls=0,resource_checks=0;
    std::string reason;
};
// Pure proposal: neither an executor nor a capacity ledger is mutated here.
// The request starts after s.delay and lasts length slots. s.other_units
// includes all OLD bookings; this helper adds the tentative request itself.
// Caller must provide the full old calendar through required_horizon and all
// old lease ends. Only the constant pulse amount varies; the tail is fixed B.
inline RequestPlan request_plan(const Snapshot&s,int request_units,int length,int B) {
    if(request_units<=0||length<1||s.capacity<1||s.units<1||s.delay<0||s.base<0||
       s.maximum<s.base||s.maximum>s.units||s.old_final_change<0||B<s.base||B>s.maximum||
       s.delay>std::numeric_limits<int>::max()-length)
        throw std::invalid_argument("Invalid request-plan parameters");
    RequestPlan d;d.term=length;d.tail=B;
    for(const auto&p:s.pending)if(p.deadline<=0){d.reason="exhausted_pending_deadline";return d;}
    d.horizon=std::max(required_horizon(s.delay+length,s.envelopes,s.pending),static_cast<int>(s.other_units.size()));
    if(s.old_units.size()<static_cast<std::size_t>(d.horizon)||s.other_units.size()<static_cast<std::size_t>(d.horizon))
        throw std::invalid_argument("Complete request admission calendars required");
    int cap=s.maximum;
    for(int n=0;n<d.horizon;++n){
        if(s.old_units[n]<s.base||s.old_units[n]>s.maximum||s.other_units[n]<0||s.other_units[n]>s.units)
            throw std::invalid_argument("Invalid request admission calendar units");
        ++d.resource_checks;
        if(n>=s.delay && n<s.delay+length){cap=std::min(cap,s.units-s.other_units[n]-request_units);}
        else if((n<s.delay?s.old_units[n]:B)>s.units-s.other_units[n]){
            d.reason="fixed_prefix_or_tail_resource_conflict";return d;
        }
    }
    d.capacity_cap=cap;
    if(cap<s.base){d.reason="no_capacity_for_candidate_pulse";return d;}
    for(long long amount=s.base;amount<=cap;++amount){
        const int u=static_cast<int>(amount);
        std::vector<long double> service(d.horizon);
        for(int n=0;n<d.horizon;++n){const int units=n<s.delay?s.old_units[n]:(n<s.delay+length?u:B);
            service[n]=floor_service(s,units);}
        ++d.certificate_calls;
        if(plan_certificate(s.envelopes,s.pending,service,s.delay+length,floor_service(s,B)).feasible){
            d.feasible=true;d.target=u;d.reason="certified_request_duration_plan";return d;
        }
    }
    d.reason="no_deadline_feasible_pulse_within_capacity_cap";return d;
}
}
#endif
