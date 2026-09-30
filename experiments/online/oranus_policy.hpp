#ifndef GUARDED_RELEASE_ORANUS_POLICY_HPP
#define GUARDED_RELEASE_ORANUS_POLICY_HPP
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace guarded_release {
// Mechanism adaptation of ORANUS (INFOCOM 2024), Section IV-D, Algorithm 3,
// Figure 3. This is NOT a reproduction of ORANUS's SNC/MDN optimizer,
// multi-service donor allocation, frequency-domain channel model, or scheduler.
// Caller supplies the actual head packet's offered age and original deadline.
// With heterogeneous deadlines, normalization is an explicit adaptation.
class OranusRtAdapted {
public:
    enum class State { A, B, C };
    struct Result { State state; int requested_extra; int desired_floor; };
private:
    double lower_,upper_;
    int base_,maximum_,requested_=0;
    State state_=State::A;
public:
    OranusRtAdapted(double lower=.3,double upper=.75,int base=21,int maximum=74)
        :lower_(lower),upper_(upper),base_(base),maximum_(maximum) {
        if(!std::isfinite(lower)||!std::isfinite(upper)||lower<=0||upper>1||lower>=upper||base<0||maximum<base)
            throw std::invalid_argument("Invalid ORANUS-inspired thresholds/floor bounds");
    }
    Result observe(bool has_head,int offered_age,int original_relative_deadline) {
        if(has_head&&(offered_age<0||original_relative_deadline<=0))
            throw std::invalid_argument("Invalid HOL packet age/deadline");
        // Explicit equality convention: upper boundary enters B; lower
        // boundary holds C. Empty queues return A. Original plotted transition
        // regions do not specify the two equality cases separately.
        const double q=has_head?static_cast<double>(offered_age)/original_relative_deadline:0;
        if(!has_head || (state_!=State::A && q<lower_)) {state_=State::A;requested_=0;}
        else if(q>=upper_) {state_=State::B;if(requested_<maximum_-base_)++requested_;}
        else if(state_==State::A) {requested_=0;}
        else {state_=State::C;}
        return {state_,requested_,base_+requested_};
    }
    State state()const {return state_;}
    int requested_extra()const {return requested_;}
    static const char*name(State s) {return s==State::A?"A":(s==State::B?"B":"C");}
};
}
#endif
