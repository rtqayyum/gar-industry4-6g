// Matched snapshot experiment. The production certificate/search are included
// unchanged; the dense oracle below is deliberately independent of their loops.
#include "../gateway/request_admission.hpp"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
using namespace guarded_release;

bool dense(const Snapshot& s, const std::vector<int>& calendar, int final_change) {
    const int H=static_cast<int>(calendar.size());
    long double rate=0;
    for(const auto& e:s.envelopes) rate+=e.rho;
    const long double tail=std::floor((long double)s.capacity*calendar.back()/s.units);
    if(rate>tail || H<final_change+s.envelopes.back().min_deadline+1) return false;
    for(const auto&e:s.envelopes) {
        long double demand=0;
        for(const auto& f:s.envelopes) {
            int n=std::max(0,e.min_deadline-f.min_deadline+1);
            if(n) demand+=f.sigma+f.rho*n;
        }
        if(demand>tail*e.min_deadline) return false;
    }
    for(int i=final_change;i<H;++i) if(calendar[i]!=calendar.back()) return false;
    std::vector<long double> prefix(H+1,0);
    for(int i=0;i<H;++i) prefix[i+1]=prefix[i]+std::floor((long double)s.capacity*calendar[i]/s.units);
    for(int a=0;a<H;++a) for(int b=a+1;b<=H;++b) {
        long double demand=0;
        if(a==0) for(const auto&p:s.pending) if(p.deadline<=b) demand+=p.remaining;
        for(const auto&e:s.envelopes) {
            int releases=0;
            // Count release slots, independently of interval_demand's formula.
            int first=std::max(1,a), last=b-e.min_deadline;
            if(last>=first) releases=last-first+1;
            if(releases) demand+=e.sigma+e.rho*releases;
        }
        if(demand>prefix[b]-prefix[a]) return false;
    }
    return true;
}

int main() {
    std::ios::sync_with_stdio(false);
    std::cout<<std::setprecision(12);
    int delay,old_final,q,length,np,H,nr;
    while(std::cin>>delay>>old_final>>q>>length>>np>>H>>nr) {
        Snapshot s; s.capacity=650;s.units=106;s.base=21;s.maximum=74;
        s.delay=delay;s.old_final_change=old_final;
        s.envelopes={{15,480,94},{45,3328,104},{95,2560,100}};
        for(int i=0;i<np;++i) {int d,w;std::cin>>d>>w;s.pending.push_back({d,(long double)w});}
        s.old_units.resize(H);s.other_units.resize(H);
        for(auto& x:s.old_units) std::cin>>x;
        for(auto& x:s.other_units) std::cin>>x;
        std::vector<std::pair<int,int>> release;
        for(int i=0;i<nr;++i) {int a,w;std::cin>>a>>w;release.push_back({a,w});}
        std::vector<long double> service(H);
        bool resources=true;
        for(int n=0;n<H;++n) {
            service[n]=floor_service(s,s.old_units[n]);
            resources=resources&&s.old_units[n]+s.other_units[n]<=s.units;
        }
        const auto initial=plan_certificate(s.envelopes,s.pending,service,old_final,service.back());
        bool old_oracle=dense(s,s.old_units,old_final);
        const bool eligible=initial.feasible&&resources;
        std::cout<<"{\"eligible\":"<<eligible<<",\"old_resources\":"<<resources
                 <<",\"old_certificate\":"<<initial.feasible<<",\"old_oracle\":"<<old_oracle
                 <<",\"old_reason\":\""<<initial.reason<<"\",\"decisions\":[";
        const int peak=*std::max_element(s.old_units.begin(),s.old_units.end());
        for(int policy=0;policy<3;++policy) {
            const auto began=std::chrono::steady_clock::now();
            bool releases_ok=!release.empty();int remaining=0,prior=-1;
            for(auto [a,w]:release) {remaining+=w;releases_ok=releases_ok&&a>prior&&a>=0&&a<length&&w>0;prior=a;}
            for(auto [a,w]:release) {releases_ok=releases_ok&&remaining<=int(floor_service(s,q))*(length-a);remaining-=w;}
            bool fits=true;
            for(int n=delay;n<delay+length;++n) fits=fits&&((policy==0?peak:s.old_units[n])+s.other_units[n]+q<=s.units);
            bool accepted=eligible&&releases_ok&&fits,replaced=false;
            int target=-1,tail=s.old_units.back(),calls=0,final=old_final;
            std::string reason=accepted?"fits_retained_calendar":(!eligible?"initial_state_not_certified":(!releases_ok?"release_suffix":"capacity"));
            std::vector<int> calendar=s.old_units;
            if(policy==2&&eligible&&releases_ok) {
                if(fits) {calls=1;accepted=plan_certificate(s.envelopes,s.pending,service,old_final,service.back()).feasible;}
                else {
                    auto plan=request_plan(s,q,length,minimum_tail(s));
                    calls=plan.certificate_calls;accepted=plan.feasible;reason=plan.reason;
                    if(accepted) {
                        replaced=true;target=plan.target;tail=plan.tail;final=delay+length;
                        for(int n=delay;n<H;++n) calendar[n]=n<final?target:tail;
                    }
                }
            }
            const double elapsed=std::chrono::duration<double,std::micro>(std::chrono::steady_clock::now()-began).count();
            bool admitted_resources=true;
            for(int n=0;n<H;++n) admitted_resources=admitted_resources&&calendar[n]+s.other_units[n]+(accepted&&n>=delay&&n<delay+length?q:0)<=s.units;
            bool oracle=dense(s,calendar,final);
            if(policy) std::cout<<",";
            std::cout<<"{\"accepted\":"<<accepted<<",\"replaced\":"<<replaced
                     <<",\"target\":"<<target<<",\"tail\":"<<tail<<",\"calls\":"<<calls
                     <<",\"reason\":\""<<reason<<"\",\"decision_us\":"<<elapsed
                     <<",\"release_pass\":"<<releases_ok<<",\"calendar_oracle\":"<<oracle
                     <<",\"resources\":"<<admitted_resources<<"}";
        }
        std::cout<<"]}"<<std::endl;
    }
}
