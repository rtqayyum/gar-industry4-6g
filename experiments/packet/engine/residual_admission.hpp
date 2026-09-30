#ifndef GUARDED_RELEASE_RESIDUAL_ADMISSION_HPP
#define GUARDED_RELEASE_RESIDUAL_ADMISSION_HPP
#include "deadline_certificate.hpp"
#include <cstdint>
namespace guarded_release {
// A separate request-aware comparator. Priority dispatch, after tenant quotas
// and background floors, guarantees this entire residual to critical EDF.
// It does not search or install a constant pulse. Inputs are causally known.
inline CheckResult residual_admission_certificate(
    const std::vector<Envelope>& contracts, const std::vector<PendingJob>& pending,
    int capacity, int background_bytes, const std::vector<std::int64_t>& old_bytes,
    int request_offset, int request_term, std::int64_t request_bytes,
    int final_change) {
    if(capacity<=0||background_bytes<0||background_bytes>=capacity||request_offset<0||
       request_term<1||request_offset>std::numeric_limits<int>::max()-request_term||
       request_bytes<0||final_change<request_offset+request_term)
        throw std::invalid_argument("Invalid residual admission input");
    const int H=required_horizon(final_change,contracts,pending);
    if(old_bytes.size()<static_cast<std::size_t>(H))
        throw std::invalid_argument("Incomplete residual calendar");
    std::vector<long double> service(old_bytes.size());
    for(std::size_t n=0;n<old_bytes.size();++n){
        const auto added=n>=static_cast<std::size_t>(request_offset)&&n<static_cast<std::size_t>(request_offset+request_term)?request_bytes:0;
        if(old_bytes[n]<0||old_bytes[n]>capacity-background_bytes||added>capacity-background_bytes-old_bytes[n])
            return {false,static_cast<int>(n),static_cast<int>(n+1),0,0,"residual_capacity_conflict",0};
        service[n]=capacity-background_bytes-old_bytes[n]-added;
    }
    return plan_certificate(contracts,pending,service,final_change,capacity-background_bytes);
}
}
#endif
