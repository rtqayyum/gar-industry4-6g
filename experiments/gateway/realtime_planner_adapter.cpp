// Persistent text/JSON bridge for the shared controller and plan executor.
// The Python gateway records actual event application times separately. This
// adapter's integer calendar never awards retroactive packet service.
#include "controller.hpp"
#include "plan_executor.hpp"
#include "request_admission.hpp"
#include "background_admission_gate.hpp"
#include "residual_admission.hpp"
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
using namespace guarded_release;

static std::string quote(const std::string& x) {
    std::string y="\""; for(char c:x) {if(c=='\\'||c=='\"')y+='\\';y+=c;}return y+'\"';
}
struct Event {long long when; std::string kind; int units; unsigned long long plan;};
static void advance_events(TimedPlanExecutor& e,long long t,std::vector<Event>& events) {
    while(true) {
        auto p=e.pending_install_time(),a=e.active_expiry();
        if((!p||*p>t)&&(!a||*a>t))break;
        const bool install=p&&*p<=t&&(!a||*p<=*a);
        const long long at=install?*p:*a;
        e.advance(at);
        events.push_back({at,install?"installation":"fallback",e.current_units(),e.active_plan_id()});
    }
    e.advance(t);
}
static void dump(TimedPlanExecutor& e,int H,const std::vector<Event>& events,int accepted=-1) {
    const auto& c=e.counters();
    auto p=e.pending_install_time(),a=e.active_expiry();
    auto pending_id=e.pending_plan_id();
    std::cout<<"{\"now\":"<<e.now()<<",\"units\":"<<e.current_units()<<",\"pending\":"<<(e.has_pending()?"true":"false")
      <<",\"pending_at\":"<<(p?*p:-1)<<",\"expiry\":"<<(a?*a:-1)<<",\"active_plan_id\":"<<e.active_plan_id()
      <<",\"pending_plan_id\":"<<(pending_id?static_cast<long long>(*pending_id):-1)
      <<",\"state_version\":"<<(c.accepted_plan_commands+c.timer_updates+c.backup_activations)
      <<",\"accepted\":"<<accepted<<",\"events\":[";
    for(std::size_t i=0;i<events.size();++i){if(i)std::cout<<',';auto&v=events[i];std::cout<<"{\"scheduled_ms\":"<<v.when<<",\"kind\":"<<quote(v.kind)<<",\"units\":"<<v.units<<",\"plan_id\":"<<v.plan<<'}';}
    std::cout<<"],\"calendar\":[";int last=e.current_units(),final_change=0;
    for(int n=0;n<H;++n){int u=e.units_at(e.now()+n);if(n)std::cout<<',';std::cout<<u;if(u!=last){final_change=n;last=u;}}
    std::cout<<"],\"old_final_change\":"<<final_change<<",\"counters\":{\"plan_commands\":"<<c.plan_commands
      <<",\"accepted_plan_commands\":"<<c.accepted_plan_commands<<",\"rejected_commands\":"<<c.rejected_commands
      <<",\"timer_updates\":"<<c.timer_updates<<",\"actual_floor_changes\":"<<c.actual_floor_changes
      <<",\"backup_activations\":"<<c.backup_activations<<",\"renewals\":"<<c.renewals
      <<",\"superseded_before_expiry\":"<<c.superseded_before_expiry<<"}}\n"<<std::flush;
}
int main(){
    std::cout<<std::setprecision(18);std::string line;std::unique_ptr<TimedPlanExecutor> executor;
    while(std::getline(std::cin,line)) {
        try {
            std::istringstream in(line);std::string op;in>>op;
            if(op=="QUIT")return 0;
            if(op=="INIT") {int u;in>>u;executor=std::make_unique<TimedPlanExecutor>(u);dump(*executor,0,{});}
            else if(op=="ADV") {long long t;int H;in>>t>>H;if(!executor||H<0||H>100000)throw std::invalid_argument("Invalid executor request");std::vector<Event> events;advance_events(*executor,t,events);dump(*executor,H,events);}
            else if(op=="PROP") {
                long long now,delay,term;int target,tail,authorized,H;in>>now>>delay>>target>>term>>tail>>authorized>>H;
                if(!executor||H<0||H>100000)throw std::invalid_argument("Invalid proposal request");
                std::vector<Event> events;advance_events(*executor,now,events);
                bool accepted=executor->propose(now,delay,target,term,tail,authorized);
                if(accepted&&delay==0)events.push_back({now,"installation",executor->current_units(),executor->active_plan_id()});
                dump(*executor,H,events,accepted?1:0);
            }
            else if(op=="GATE") {
                HeadroomInput s;int J,H;long double eta;
                in>>s.capacity>>s.request_offset>>s.request_term>>s.request_bytes_per_slot>>eta>>J>>H;
                if(J<0||J>100000||H<1||H>100000||eta<0||!std::isfinite(eta))throw std::invalid_argument("Invalid gate dimensions");
                int D[7];std::int64_t amount[7];for(auto&x:D)in>>x;for(auto&x:amount)in>>x;
                for(int k=0;k<7;++k){if(amount[k]<0)throw std::invalid_argument("Negative arrival history");s.forecast.push_back({D[k],0,eta*amount[k]/OriginalArrivalWindow::width});}
                std::stable_sort(s.forecast.begin(),s.forecast.end(),[](auto&a,auto&b){return a.min_deadline<b.min_deadline;});
                for(int j=0;j<J;++j){PendingJob p;in>>p.deadline>>p.remaining;s.pending.push_back(p);}
                for(int n=0;n<H;++n){std::int64_t b;in>>b;s.booked_service.push_back(b);}
                if(!in)throw std::invalid_argument("Incomplete gate snapshot");
                const auto began=std::chrono::steady_clock::now();const auto d=deadline_headroom_gate(s);
                const auto us=std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-began).count();
                std::cout<<"{\"pass\":"<<(d.pass?"true":"false")<<",\"horizon\":"<<d.horizon<<",\"checks\":"<<d.interval_checks
                    <<",\"failing_start\":"<<d.failing_start<<",\"failing_end\":"<<d.failing_end<<",\"demand\":"<<d.demand
                    <<",\"available\":"<<d.available<<",\"planning_us\":"<<us<<",\"reason\":"<<quote(d.reason)<<"}\n"<<std::flush;
            }
            else if(op=="RESIDUAL") {
                int C,bg,offset,L,final_change,K,J,H;std::int64_t added;in>>C>>bg>>offset>>L>>added>>final_change>>K>>J>>H;
                if(K<1||K>100||J<0||J>100000||H<1||H>100000)throw std::invalid_argument("Invalid residual snapshot dimensions");
                std::vector<Envelope> env;std::vector<PendingJob> pending;std::vector<std::int64_t> old;
                for(int k=0;k<K;++k){Envelope e;in>>e.min_deadline>>e.sigma>>e.rho;env.push_back(e);}
                for(int j=0;j<J;++j){PendingJob p;in>>p.deadline>>p.remaining;pending.push_back(p);}
                for(int n=0;n<H;++n){std::int64_t b;in>>b;old.push_back(b);}
                if(!in)throw std::invalid_argument("Incomplete residual snapshot");
                const auto began=std::chrono::steady_clock::now();const auto d=residual_admission_certificate(env,pending,C,bg,old,offset,L,added,final_change);
                const auto us=std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-began).count();
                std::cout<<"{\"pass\":"<<(d.feasible?"true":"false")<<",\"checks\":"<<d.checks<<",\"planning_us\":"<<us
                    <<",\"reason\":"<<quote(d.reason)<<"}\n"<<std::flush;
            }
            else if(op=="PLAN"||op=="JOINT"||op=="PRED"||op=="CHECK") {
                Snapshot s;int K,J,H;in>>s.capacity>>s.units>>s.base>>s.maximum>>s.step>>s.delay>>s.term>>s.old_final_change>>s.command_price>>K>>J>>H;
                if(K<0||J<0||H<1||K>100||J>100000||H>100000)throw std::invalid_argument("Invalid snapshot dimensions");
                for(int k=0;k<K;++k){Envelope e;in>>e.min_deadline>>e.sigma>>e.rho;s.envelopes.push_back(e);}
                for(int j=0;j<J;++j){PendingJob p;in>>p.deadline>>p.remaining;s.pending.push_back(p);}
                for(int n=0;n<H;++n){int u;in>>u;s.old_units.push_back(u);}
                for(int n=0;n<H;++n){int u;in>>u;s.other_units.push_back(u);}
                if(!in)throw std::invalid_argument("Incomplete snapshot");
                if(op=="CHECK") {
                    int target,term,tail;in>>target>>term>>tail;
                    if(!in||s.delay<0||term<1||target<s.base||target>s.maximum||tail<s.base||tail>s.maximum)
                        throw std::invalid_argument("Invalid bound candidate");
                    const int required=required_horizon(s.delay+term,s.envelopes,s.pending);
                    bool resources=true;
                    if(H<required)throw std::invalid_argument("Incomplete candidate horizon");
                    std::vector<long double> service(H);
                    for(int n=0;n<H;++n) {
                        const int u=n<s.delay?s.old_units[n]:(n<s.delay+term?target:tail);
                        resources=resources&&(u+s.other_units[n]<=s.units);
                        service[n]=floor_service(s,u);
                    }
                    const auto check=plan_certificate(s.envelopes,s.pending,service,s.delay+term,floor_service(s,tail));
                    std::cout<<"{\"pass\":"<<((resources&&check.feasible)?"true":"false")
                             <<",\"reason\":"<<quote(resources?"candidate_deadline_certificate":"candidate_resource_conflict")<<"}\n"<<std::flush;
                    continue;
                }
                if(op=="JOINT") {
                    int q,L;in>>q>>L;if(!in)throw std::invalid_argument("Incomplete tenant request");
                    const auto began=std::chrono::steady_clock::now();
                    const int B=minimum_tail(s);if(B<0)throw std::invalid_argument("Unsustainable request contract");
                    auto d=request_plan(s,q,L,B);
                    const auto us=std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-began).count();
                    std::cout<<"{\"certified\":"<<(d.feasible?"true":"false")<<",\"replace\":"<<(d.feasible?"true":"false")
                      <<",\"no_feasible\":"<<(d.feasible?"false":"true")<<",\"target\":"<<d.target<<",\"term\":"<<d.term
                      <<",\"tail\":"<<d.tail<<",\"horizon\":"<<d.horizon<<",\"feasibility_calls\":"<<d.certificate_calls
                      <<",\"planning_us\":"<<us<<",\"reason\":"<<quote(d.reason)<<"}\n"<<std::flush;
                    continue;
                }
                s.allow_release=op!="PRED";
                auto d=guarded_decision(s);
                std::cout<<"{\"certified\":"<<(d.certified?"true":"false")<<",\"replace\":"<<(d.replace?"true":"false")
                  <<",\"release\":"<<(d.release?"true":"false")<<",\"no_feasible\":"<<(d.no_feasible?"true":"false")
                  <<",\"target\":"<<d.target<<",\"term\":"<<d.term<<",\"tail\":"<<d.tail<<",\"horizon\":"<<d.horizon
                  <<",\"feasibility_calls\":"<<d.feasibility_calls<<",\"old_cost\":"<<d.old_cost<<",\"proposed_cost\":"<<d.proposed_cost
                  <<",\"planning_us\":"<<d.planning_us<<",\"reason\":"<<quote(d.reason)<<"}\n"<<std::flush;
            } else throw std::invalid_argument("Unknown adapter operation");
        } catch(const std::exception& e){std::cout<<"{\"error\":"<<quote(e.what())<<"}\n"<<std::flush;}
    }
}
