// Online gateway planning and admission benchmark.
#include "workload.hpp"
#include "controller.hpp"
#include "plan_executor.hpp"
#include "oranus_policy.hpp"
#include "request_admission.hpp"
#include "background_admission_gate.hpp"
#include "residual_admission.hpp"
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <queue>
#include <set>

using namespace guarded_release;
using Count = long long;
namespace {
constexpr int N=106, BASE=21, MAXIMUM=74, DRAIN=1001;
constexpr Count BUFFER=16LL*1024*1024;
struct Options {
    std::string context,output,policy="guarded",profile="industrial",packet_export,lease_export,offer_trace_export;
    int duration=10000,seed=1001,capacity=650,step=5,delay=2,term=40;
    int shaping_budget=5,guardband=0,fixed=-1,dwell=20,lease_term=0,outage_start=0,outage_length=0;
    int batch_size=1;
    double load=1,burst_factor=2.5,price=0,contract_factor=-1,enter=2,exit=.33,drain_target=10,lease_rate=0,hol_upper=.75,hol_lower=.3;
    double gate_eta=-1;
    std::string family="original";int expire_background=0;
};
Options parse(int argc,char**argv){
    Options o;
    for(int i=1;i<argc;++i){std::string k=argv[i];
        if(k=="--help") {std::cout<<"Guarded-release packet simulator. Required: --context CSV --output JSON.\n"
            "--policy guarded|priority_guarded|shared|priority|fixed_soft|hysteresis|predictive|oranus_rt_adapted|joint_guarded|priority_joint\n"
            "--duration MS --seed N --load X --burst-factor X --capacity BYTES_PER_MS\n"
            "--step MS --delay MS --term MS --price UNIT_MS --contract-factor X\n"
            "--shaping-budget MS --guardband MS --profile industrial|urllc --fixed UNITS\n"
            "--enter BACKLOG_CAPACITY_SLOTS --exit BACKLOG_CAPACITY_SLOTS\n"
            "--drain-target MS --dwell MS --hol-upper FRACTION --hol-lower FRACTION --lease-rate REQUESTS_PER_SECOND\n"
            "--lease-term 0_OR_MS --outage-start MS --outage-length MS --packet-export CSV --lease-export CSV --lease-offer-trace-export CSV\n"
            "--batch-size N (online comparator adapters)\n"
            "--gate-eta -1_OR_NONNEGATIVE (-1=off; fixed observed window100ms)\n";std::exit(0);}
        if(i+1==argc)throw std::invalid_argument("Missing option value: "+k);std::string v=argv[++i];
        if(k=="--family")o.family=v;else if(k=="--expire-background")o.expire_background=std::stoi(v);else if(k=="--batch-size")o.batch_size=std::stoi(v);else if(k=="--lease-offer-trace-export")o.offer_trace_export=v;else if(k=="--context")o.context=v;else if(k=="--output")o.output=v;
        else if(k=="--policy")o.policy=v;else if(k=="--profile")o.profile=v;else if(k=="--packet-export")o.packet_export=v;else if(k=="--lease-export")o.lease_export=v;
        else if(k=="--duration")o.duration=std::stoi(v);else if(k=="--seed")o.seed=std::stoi(v);
        else if(k=="--capacity")o.capacity=std::stoi(v);else if(k=="--step")o.step=std::stoi(v);
        else if(k=="--delay")o.delay=std::stoi(v);else if(k=="--term")o.term=std::stoi(v);
        else if(k=="--shaping-budget")o.shaping_budget=std::stoi(v);else if(k=="--guardband")o.guardband=std::stoi(v);
        else if(k=="--fixed")o.fixed=std::stoi(v);else if(k=="--dwell")o.dwell=std::stoi(v);
        else if(k=="--lease-term")o.lease_term=std::stoi(v);else if(k=="--outage-start")o.outage_start=std::stoi(v);
        else if(k=="--outage-length")o.outage_length=std::stoi(v);else if(k=="--load")o.load=std::stod(v);
        else if(k=="--burst-factor")o.burst_factor=std::stod(v);else if(k=="--price")o.price=std::stod(v);
        else if(k=="--contract-factor")o.contract_factor=std::stod(v);else if(k=="--enter")o.enter=std::stod(v);
        else if(k=="--exit")o.exit=std::stod(v);else if(k=="--drain-target")o.drain_target=std::stod(v);
        else if(k=="--hol-upper")o.hol_upper=std::stod(v);else if(k=="--hol-lower")o.hol_lower=std::stod(v);
        else if(k=="--gate-eta")o.gate_eta=std::stod(v);
        else if(k=="--lease-rate")o.lease_rate=std::stod(v);else throw std::invalid_argument("Unknown option: "+k);
    }
    if(!std::set<std::string>{"original","synchronized","deadline_mix","heavy_tenants","regime_shift"}.count(o.family)||(o.expire_background!=0&&o.expire_background!=1))throw std::invalid_argument("Unknown workload family or expiry mode");
    if(o.family=="deadline_mix")o.profile="mixed";
    const std::set<std::string> policies={"guarded","priority_guarded","shared","priority","fixed_soft","hysteresis","predictive","oranus_rt_adapted","joint_guarded","priority_joint","residual_priority","luu_joint_style","luu_sequential_style","yu_gateway_style"};
    if(o.context.empty()||o.output.empty()||!policies.count(o.policy))throw std::invalid_argument("Required paths or policy invalid");
    if(o.duration<1||o.duration>100000000||o.capacity<1||o.capacity>100000000||o.step<1||o.delay<0||o.term<1||
       o.delay>100000||o.term>100000||o.shaping_budget<0||o.guardband<0||o.dwell<0||o.lease_term<0||o.lease_term>1000||
       o.outage_start<0||o.outage_length<0||o.outage_length>100000000||o.batch_size<1||o.batch_size>6|| (o.fixed!=-1&&(o.fixed<BASE||o.fixed>MAXIMUM)))
        throw std::invalid_argument("Integer option outside supported range");
    for(double x:{o.load,o.burst_factor,o.price,o.contract_factor,o.enter,o.exit,o.drain_target,o.lease_rate,o.hol_upper,o.hol_lower,o.gate_eta})
        if(!std::isfinite(x))throw std::invalid_argument("Nonfinite numeric option");
    if(o.load<=0||o.burst_factor<=0||o.price<0||o.enter<0||o.exit<0||o.exit>o.enter||o.drain_target<=0||o.lease_rate<0||o.lease_rate>1000||
       (o.contract_factor!=-1&&o.contract_factor<=0)||(o.gate_eta!=-1&&o.gate_eta<0)||o.hol_lower<=0||o.hol_upper>1||o.hol_lower>=o.hol_upper)throw std::invalid_argument("Invalid real option");
    if(o.contract_factor<0)o.contract_factor=1.4*o.load*std::max(1.0,o.burst_factor)/.9;
    return o;
}
std::string quote(const std::string&s){std::ostringstream o;o<<'"';for(unsigned char c:s){
    if(c=='"'||c=='\\')o<<'\\'<<c;else if(c=='\n')o<<"\\n";else if(c=='\r')o<<"\\r";else if(c=='\t')o<<"\\t";
    else if(c<32)o<<"\\u"<<std::hex<<std::setw(4)<<std::setfill('0')<<int(c)<<std::dec;else o<<c;}o<<'"';return o.str();}
struct Json {
    std::ostream&out;bool first=true;explicit Json(std::ostream&s):out(s){out<<'{';}
    void key(const std::string&k){if(!first)out<<',';first=false;out<<'\n'<<quote(k)<<':';}
    template<class T>void number(const std::string&k,T v){key(k);out<<std::setprecision(18)<<v;}
    void text(const std::string&k,const std::string&v){key(k);out<<quote(v);}
    void boolean(const std::string&k,bool v){key(k);out<<(v?"true":"false");}
    void close(){out<<"\n}";}
};
struct Stats {
    Count generated=0,generated_bytes=0,queue_admitted=0,shaper_accepted=0,ingress_dropped=0,overflow=0,expired=0;
    Count completed=0,on_time=0,on_time_bytes=0,served_bytes=0,active_served_bytes=0,unfinished=0,failures=0;
};
void stats_json(std::ostream&out,const Stats&s){Json j(out);
#define FIELD(x) j.number(#x,s.x)
    FIELD(generated);FIELD(generated_bytes);FIELD(queue_admitted);FIELD(shaper_accepted);FIELD(ingress_dropped);
    FIELD(overflow);FIELD(expired);FIELD(completed);FIELD(on_time);FIELD(on_time_bytes);FIELD(served_bytes);
    FIELD(active_served_bytes);FIELD(unfinished);FIELD(failures);
#undef FIELD
    j.close();}
struct Outcome {Packet p;int remaining,queue_arrival=-1,completion=-1;bool admitted=false;std::string status="not_offered";};
struct Earlier {const std::vector<Outcome>*v=nullptr;bool operator()(int a,int b)const{
    const auto&x=(*v)[a].p;const auto&y=(*v)[b].p;return x.deadline!=y.deadline?x.deadline>y.deadline:x.id>y.id;}};
struct Lease {int offer,start,end,units;Count work,remaining;bool completed=false;int completion=-1;};
struct LeaseOffer {int offer,start,end,units;Count work;bool accepted=false,joint=false;int index=-1;bool gate_tested=false,gate_passed=false;int priority=0;};
long double quantile(std::vector<long double>v,long double q){if(v.empty())return 0;std::sort(v.begin(),v.end());return v[static_cast<std::size_t>(std::ceil(q*v.size()))-1];}

struct Simulation {
    Options o;OranusRtAdapted oranus;WorkloadOptions w;std::vector<Envelope> contracts;std::vector<Packet> offered;
    std::vector<Outcome> outcomes;std::array<Stats,3> classes{};std::array<Stats,7> types{};std::vector<Stats> sources;
    std::array<std::priority_queue<int,std::vector<int>,Earlier>,3> queues;
    std::array<Count,3> backlog{};std::array<std::vector<Count>,3> shaped_arrivals;
    std::map<int,Count> shaping_delay_hist,dropped_shaping_wait_hist;
    std::vector<Lease> leases;std::vector<LeaseOffer> lease_offers;std::vector<int> ledger;std::map<std::string,Count> count;
    std::vector<Count> tenant_byte_ledger;
    OriginalArrivalWindow original_history;
    std::set<int> known_ingress;
    std::vector<long double> gate_times;
    std::vector<long double> planning_times,admission_planning_times;std::vector<int> failure_deadlines;
    int B=-1,initial=BASE,observation,latest_lease_end=0,last_hysteresis_command=-1000000000;bool protected_mode=true;
    bool initial_plan_certified=false;Count shape_delay_sum=0,shape_delay_max=0,active_lease_bytes=0,lease_bytes=0;
    Count actual_area=0,promised_area100=0,standby_area=0,undrawn_timed_area=0,unconfigured_rights_area=0,active_used=0,total_used=0,critical_admitted_failures=0;
    Count queue_end_bytes=0,partial_failed_critical_bytes=0;
    explicit Simulation(const Options&options):o(options),oranus(o.hol_lower,o.hol_upper),observation(o.duration+std::max(DRAIN,o.delay+(o.lease_term?o.lease_term:100)+1)){
        w.seed=o.seed;w.duration=o.duration;w.load=o.load;w.burst_factor=o.burst_factor;w.profile=o.profile;w.family=o.family;
        contracts=source_contracts(w,o.shaping_budget,o.guardband,o.contract_factor);
        Snapshot init;init.capacity=o.capacity;init.envelopes=contracts;B=minimum_tail(init);
        if(B<0)return;
        if(o.fixed<0)o.fixed=B;
        initial=(o.policy=="shared"||o.policy=="residual_priority")?BASE:(o.policy=="fixed_soft"?o.fixed:B);
        std::vector<long double> static_service(required_horizon(0,contracts,{}),(o.policy=="residual_priority"?o.capacity-o.capacity*27/N-o.capacity*5/N:std::floor(static_cast<long double>(o.capacity)*initial/N)));
        initial_plan_certified=plan_certificate(contracts,{},static_service,0,static_service.back()).feasible;
        offered=generate_packets(o.context,w);outcomes.resize(offered.size());int max_source=0;
        for(const auto&p:offered){outcomes[p.id]={p,p.bytes};max_source=std::max(max_source,p.source);}
        sources.resize(max_source+1);
        for(auto&q:queues)q=std::priority_queue<int,std::vector<int>,Earlier>(Earlier{&outcomes});
        for(auto&a:shaped_arrivals)a.assign(observation,0);
        ledger.assign(observation+1002,0);
        tenant_byte_ledger.assign(ledger.size(),0);
    }
    template<class F>void each(const Packet&p,F f){f(classes[p.cls]);f(types[p.type]);f(sources[p.source]);}
    void admit(const Packet&p,int t){auto&x=outcomes[p.id];
        if(backlog[p.cls]+p.bytes>BUFFER){x.status="overflow";each(p,[](Stats&s){++s.overflow;});return;}
        x.queue_arrival=t;x.admitted=true;x.status="queued";queues[p.cls].push(p.id);backlog[p.cls]+=p.bytes;
        each(p,[](Stats&s){++s.queue_admitted;});
    }
    int booked(int t)const{return t>=0&&t<static_cast<int>(ledger.size())?ledger[t]:0;}
    Count booked_bytes(int t)const{return t>=0&&t<static_cast<int>(tenant_byte_ledger.size())?tenant_byte_ledger[t]:0;}
    HeadroomInput headroom_snapshot(int t,int units,int L)const{
        if(original_history.now()!=t)throw std::runtime_error("Headroom history does not match observed slot");
        HeadroomInput s;s.capacity=o.capacity;s.request_offset=o.delay;s.request_term=L;
        s.request_bytes_per_slot=static_cast<Count>(o.capacity)*units/N;
        const auto cd=critical_deadlines(o.profile);
        const int D[7]={cd[0],cd[1],cd[2],o.family=="deadline_mix"?150:500,o.family=="deadline_mix"?80:200,cd[1],o.family=="deadline_mix"?50:100};
        const auto rates=original_history.rates(o.gate_eta);
        for(int k=0;k<7;++k)s.forecast.push_back({D[k],0,rates[k]});
        std::stable_sort(s.forecast.begin(),s.forecast.end(),[](const auto&a,const auto&b){return a.min_deadline<b.min_deadline;});
        int last_pending=0;
        auto append=[&](const Outcome&x){
            // Late background work is still served by the unchanged dispatcher.
            // Charge that known interference immediately rather than omit it.
            const int remaining_deadline=std::max(1,x.p.deadline-t);
            s.pending.push_back({remaining_deadline,static_cast<long double>(x.remaining)});
            last_pending=std::max(last_pending,remaining_deadline);
        };
        for(const auto&queue:queues){auto q=queue;while(!q.empty()){append(outcomes[q.top()]);q.pop();}}
        // Only causally observed packets waiting at ingress are included.
        for(int id:known_ingress)append(outcomes[id]);
        const int last_booking=std::max(o.delay+L,std::max(0,latest_lease_end-t));
        const int H=std::max(last_booking+s.forecast.back().min_deadline+1,last_pending);
        s.booked_service.reserve(H);for(int n=0;n<H;++n)s.booked_service.push_back(booked_bytes(t+n));
        return s;
    }
    bool gate_request(int t,int units,int L){
        ++count["gate_calls"];const auto started=std::chrono::steady_clock::now();
        const auto decision=deadline_headroom_gate(headroom_snapshot(t,units,L));
        count["gate_interval_checks"]+=decision.interval_checks;
        ++count[decision.pass?"gate_passed":"gate_rejected"];
        gate_times.push_back(std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-started).count());
        return decision.pass;
    }
    int take(int cls,int amount,int t){int used=0;auto&q=queues[cls];
        while(amount>0&&!q.empty()){auto&x=outcomes[q.top()];const int bytes=std::min(amount,x.remaining);
            x.remaining-=bytes;backlog[cls]-=bytes;amount-=bytes;used+=bytes;
            each(x.p,[&](Stats&s){s.served_bytes+=bytes;if(t<o.duration)s.active_served_bytes+=bytes;});
            if(x.remaining==0){x.completion=t+1;x.status="completed";const bool timely=x.completion<=x.p.deadline;
                each(x.p,[&](Stats&s){++s.completed;if(timely){++s.on_time;s.on_time_bytes+=x.p.bytes;}});q.pop();}
        }return used;
    }
    int spare_weighted(int available,int t,bool background_only=false){int total=0;const int weights[3]={20,65,15};
        while(available>0){int weight=0,first=-1;for(int k=background_only?1:0;k<3;++k)if(backlog[k]>0){weight+=weights[k];if(first<0)first=k;}
            if(!weight)break;const int round=available;int used=0;
            for(int k=background_only?1:0;k<3;++k)if(backlog[k]>0)used+=take(k,static_cast<int>(static_cast<Count>(round)*weights[k]/weight),t);
            if(!used)used=take(first,1,t);available-=used;total+=used;
        }return total;
    }
    Snapshot snapshot(int t,const TimedPlanExecutor&executor,int admission_term=0){Snapshot s;s.now=t;s.capacity=o.capacity;s.step=o.step;s.delay=o.delay;s.term=o.term;
        s.command_price=o.price;s.allow_release=o.policy!="predictive";s.envelopes=contracts;
        if(auto expiry=executor.active_expiry())s.old_final_change=static_cast<int>(*expiry-t);
        auto q=queues[0];while(!q.empty()){const auto&x=outcomes[q.top()];s.pending.push_back({x.p.deadline-t-o.guardband,static_cast<long double>(x.remaining)});q.pop();}
        const int T=std::max({s.old_final_change,o.delay+std::max({o.step,o.term,admission_term}),std::max(0,latest_lease_end-t)});
        int max_pending=0;for(const auto&p:s.pending)max_pending=std::max(max_pending,p.deadline);
        const int H=std::max(T+contracts.back().min_deadline+1,max_pending);
        s.old_units.reserve(H);s.other_units.reserve(H);
        for(int n=0;n<H;++n){s.old_units.push_back(executor.units_at(t+n));s.other_units.push_back(32+booked(t+n));}
        return s;
    }
    bool resources(const Snapshot&s,int target,int length,int tail){
        ++count["resource_plan_checks"];
        for(int n=0;n<static_cast<int>(s.old_units.size());++n){const int u=n<o.delay?s.old_units[n]:(n<o.delay+length?target:tail);
            if(u+s.other_units[n]>N)return false;}return true;
    }

    std::vector<long double> complete_control_times,complete_batch_times;
    struct CompleteTimer {
        std::vector<long double>&samples;
        std::chrono::steady_clock::time_point begin=std::chrono::steady_clock::now();
        explicit CompleteTimer(std::vector<long double>&s):samples(s){}
        ~CompleteTimer(){samples.push_back(std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-begin).count());}
    };
    void plan(int t,TimedPlanExecutor&executor){
        CompleteTimer complete_timer(complete_control_times);
        ++count["control_ticks"];
        if(executor.has_pending()){++count["skipped_pending_ticks"];return;}
        if(t>=o.outage_start&&t<static_cast<Count>(o.outage_start)+o.outage_length){++count["skipped_outage_ticks"];return;}
        if(o.policy=="shared"||o.policy=="priority"||o.policy=="fixed_soft"||o.policy=="residual_priority")return;
        ++count["planning_calls"];++count["regular_planning_calls"];const auto started=std::chrono::steady_clock::now();Snapshot s=snapshot(t,executor);
        if(o.policy=="oranus_rt_adapted"){
            // Original Algorithm 3 observes each service's HOL packet. Keep
            // separate FIFO heads for the three critical deadline groups,
            // then control their common floor using the most urgent ratio.
            std::array<int,3> heads{{-1,-1,-1}};auto q=queues[0];
            while(!q.empty()){const int id=q.top();q.pop();const auto&p=outcomes[id].p;const int group=deadline_group(p);
                if(heads[group]<0||p.arrival<outcomes[heads[group]].p.arrival||
                   (p.arrival==outcomes[heads[group]].p.arrival&&p.id<outcomes[heads[group]].p.id))heads[group]=id;}
            int age=0,D=1;bool has_head=false;
            for(int id:heads)if(id>=0){const auto&p=outcomes[id].p;const int a=t-p.arrival,d=p.deadline-p.arrival;
                if(!has_head||static_cast<Count>(a)*D>static_cast<Count>(age)*d){age=a;D=d;has_head=true;}}
            const auto decision=oranus.observe(has_head,age,D);
            ++count[std::string("oranus_state_")+OranusRtAdapted::name(decision.state)+"_observations"];
            int target=decision.desired_floor;bool approved=false;
            for(;target>=BASE;--target){if(resources(s,target,o.term,B)){approved=true;break;}}
            if(!approved){++count["resource_plan_conflicts"];executor.propose(t,o.delay,decision.desired_floor,o.term,B,false);}
            else{
                if(target<decision.desired_floor)++count["oranus_capped_requests"];
                bool identical=true;for(int n=0;n<static_cast<int>(s.old_units.size());++n){const int u=n<o.delay?s.old_units[n]:(n<o.delay+o.term?target:B);identical=identical&&u==s.old_units[n];}
                if(!identical&&executor.propose(t,o.delay,target,o.term,B,true))++count["uncertified_approved_commands"];
            }
        }else if(o.policy=="yu_gateway_style"){
            // Yu-inspired event-driven floor reclamation.  Arrival handling
            // recomputes immediately in admit_request(); this control path
            // acts only on a completed booking and never calls GAR's
            // guarded_decision().
            ++count["yu_event_control_ticks"];
            if(yu_recompute_pending){
                yu_recompute_pending=false;
                yu_recompute_minimum_floor(t,executor,"expiry");
            }
        }else if(o.policy=="hysteresis"){
            if(t-last_hysteresis_command<o.dwell){++count["dwell_retained_calls"];}
            else {
                if(backlog[0]>o.enter*o.capacity)protected_mode=true;
                else if(backlog[0]<o.exit*o.capacity)protected_mode=false;
                int target=BASE;
                if(protected_mode){long double demand_rate=backlog[0]/o.drain_target;
                    std::sort(s.pending.begin(),s.pending.end(),[](const auto&a,const auto&b){return a.deadline<b.deadline;});
                    long double cumulative=0;for(const auto&p:s.pending){cumulative+=p.remaining;demand_rate=std::max(demand_rate,cumulative/std::max(1,p.deadline-o.delay));}
                    target=std::clamp(static_cast<int>(std::min<long double>(MAXIMUM,std::ceil(demand_rate*N/o.capacity))),B,MAXIMUM);}
                bool identical=true;for(int n=0;n<static_cast<int>(s.old_units.size());++n){const int u=n<o.delay?s.old_units[n]:(n<o.delay+o.term?target:B);identical=identical&&u==s.old_units[n];}
                if(!identical){bool approved=resources(s,target,o.term,B);if(!approved)++count["resource_plan_conflicts"];
                    if(executor.propose(t,o.delay,target,o.term,B,approved)){last_hysteresis_command=t;++count["uncertified_approved_commands"];}}
            }
        }else{
            bool exhausted=false;for(const auto&p:s.pending)exhausted=exhausted||p.deadline<=0;
            if(exhausted){++count["no_feasible_plan"];++count["guardbudget_exhausted_calls"];}
            else {const Decision d=guarded_decision(s);count["certificate_calls"]+=d.feasibility_calls;
                if(d.no_feasible)++count["no_feasible_plan"];
                if(d.certified)++count["certified_decisions"];
                if(d.release)++count["release_decisions"];
                if(d.replace){if(executor.propose(t,o.delay,d.target,d.term,d.tail,d.certified)){++count["certified_approved_commands"];if(d.release)++count["release_commands"];}}
                else ++count["retained_plan_calls"];
            }
        }
        planning_times.push_back(std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-started).count());
    }

    // ---------------------------------------------------------------------
    // online published-method adapters.  These are deliberately narrow
    // one-gateway mappings, not reproductions of either cited system.
    // The common calendar/certificate guard remains independent of proposal
    // logic, so an adapter cannot install an uncertified booking.
    // Set when a finite tenant booking expires.  Yu-inspired reclamation is
    // event-driven; unlike GAR-H it does not periodically optimize between
    // arrival/expiry events.
    bool yu_recompute_pending=false;
    struct RawOfferTrace { int offer,start,end,units,term,priority,ordinal; };
    std::vector<RawOfferTrace> raw_offer_trace;
    struct TenantProposal { int units=0, term=0, priority=0, ordinal=0; };
    struct BatchPlan { bool feasible=false; int target=0, term=0; std::size_t certificate_calls=0, resource_checks=0; };

    int declared_priority(int units,int term,int ordinal) const {
        // One-hop gateway priority: shorter requested completion window wins;
        // ties favor less resource-time, then deterministic input order.
        return 1000000/std::max(1,term)*1000-units*10-ordinal;
    }

    void append_accepted_lease(int t,int start,int end,int units,Count work,LeaseOffer&record){
        for(int tau=start;tau<end;++tau)
            if(executor_for_commit==nullptr || executor_for_commit->units_at(tau)+32+booked(tau)+units>N)
                throw std::runtime_error("Accepted online booking calendar inconsistent");
        record.accepted=true;record.index=static_cast<int>(leases.size());
        leases.push_back({t,start,end,units,work,work});
        ++count["lease_accepted"];count["lease_accepted_unit_ms"]+=static_cast<Count>(units)*(end-start);count["lease_accepted_work_bytes"]+=work;
        latest_lease_end=std::max(latest_lease_end,end);
        for(int tau=start;tau<end;++tau){ledger[tau]+=units;tenant_byte_ledger[tau]+=static_cast<Count>(o.capacity)*units/N;}
    }

    // A short-lived pointer avoids a second large executor argument inside the
    // existing accounting helper.  It is set only around online batch commits.
    const TimedPlanExecutor* executor_for_commit=nullptr;

    bool online_calendar_guard(const Snapshot&s,int target,int length,int tail,std::size_t&checks){
        const int H=std::max(required_horizon(s.delay+length,s.envelopes,s.pending),static_cast<int>(s.old_units.size()));
        if(s.old_units.size()<static_cast<std::size_t>(H)||s.other_units.size()<static_cast<std::size_t>(H))
            throw std::runtime_error("online guard requires complete calendar");
        std::vector<long double> service(H);
        for(int n=0;n<H;++n){
            const int u=n<s.delay?s.old_units[n]:(n<s.delay+length?target:tail);
            if(u+s.other_units[n]>N)return false;
            service[n]=floor_service(s,u);
        }
        ++checks;
        return plan_certificate(s.envelopes,s.pending,service,s.delay+length,floor_service(s,tail)).feasible;
    }

    bool online_existing_admission_guard(int t,int units,int length,const TimedPlanExecutor&executor){
        // Common independent guard for the two sequential external adapters.
        // It checks the projected installed calendar plus the prospective
        // booking, without granting either adapter access to future arrivals.
        const Snapshot s=snapshot(t,executor,length);
        const int H=std::max(required_horizon(s.delay+length,s.envelopes,s.pending),static_cast<int>(s.old_units.size()));
        if(s.old_units.size()<static_cast<std::size_t>(H)||s.other_units.size()<static_cast<std::size_t>(H))
            throw std::runtime_error("online admission guard requires complete calendar");
        std::vector<long double> service(H);
        for(int n=0;n<H;++n){
            const int booking=n>=s.delay&&n<s.delay+length?units:0;
            if(s.old_units[n]+s.other_units[n]+booking>N)return false;
            service[n]=floor_service(s,s.old_units[n]);
        }
        ++count["adapter_common_admission_guard_calls"];
        const bool pass=plan_certificate(s.envelopes,s.pending,service,s.old_final_change,service.back()).feasible;
        if(!pass)++count["adapter_common_admission_guard_rejections"];
        return pass;
    }

    RequestPlan yu_event_request_plan(const Snapshot&s,int units,int length,int tail){
        // Yu-inspired one-gateway mapping: a newly arriving finite request
        // triggers an online local reservation recomputation and the common
        // guard certifies its full legal booking.  There is no arbitrary
        // per-tick increment cap.
        ++count["yu_event_request_attempts"];
        return request_plan(s,units,length,tail);
    }

    bool yu_recompute_minimum_floor(int t,TimedPlanExecutor&executor,const char*event){
        // Narrow Yu-inspired mapping: recompute the minimum legal gateway
        // floor for the *fixed* active booking calendar after an event.  The
        // simulator cannot redistribute service among existing tenant flows;
        // that source-system mechanism is intentionally out of scope.
        int last_end=t+o.delay;
        for(const auto&lease:leases)if(lease.end>t)last_end=std::max(last_end,lease.end);
        const int length=std::max(1,last_end-(t+o.delay));
        const Snapshot s=snapshot(t,executor,length);
        ++count["yu_event_recomputations"];
        if(std::string(event)=="expiry")++count["yu_expiry_recomputations"];
        else ++count["yu_arrival_recomputations"];
        for(int target=BASE;target<=MAXIMUM;++target){
            std::size_t checks=0;
            const bool certified=online_calendar_guard(s,target,length,B,checks);
            count["yu_event_guard_checks"]+=checks;
            if(!certified)continue;
            bool identical=true;
            for(int n=0;n<static_cast<int>(s.old_units.size());++n){
                const int u=n<o.delay?s.old_units[n]:(n<o.delay+length?target:B);
                identical=identical&&(u==s.old_units[n]);
            }
            if(!identical){
                if(executor.propose(t,o.delay,target,length,B,true)){++count["yu_event_commands"];++count["certified_approved_commands"];}
                else ++count["yu_event_guard_rejections"];
            }
            return true;
        }
        ++count["yu_event_guard_rejections"];
        return false;
    }

    void export_raw_offer_trace(){
        if(o.offer_trace_export.empty())return;
        std::ofstream f(o.offer_trace_export);
        if(!f)throw std::runtime_error("Cannot write immutable online offer trace");
        f<<"offer_ms,start_ms,end_ms,units,term_ms,priority,ordinal\n";
        for(const auto&x:raw_offer_trace)
            f<<x.offer<<','<<x.start<<','<<x.end<<','<<x.units<<','<<x.term<<','<<x.priority<<','<<x.ordinal<<'\n';
    }


    // The batch gate charges every selected tenant's individually rounded
    // byte quota in its own window. It never floors the summed resource units.
    // Existing bookings remain charged through headroom_snapshot.
    bool gate_luu_subset(int t,const std::vector<TenantProposal>&batch,unsigned mask){
        if(o.gate_eta<0)return true;
        int length=0;
        for(std::size_t i=0;i<batch.size();++i)
            if(mask&(1U<<i))length=std::max(length,batch[i].term);
        if(length<1)return false;
        ++count["luu_joint_aggregate_gate_calls"];
        const auto started=std::chrono::steady_clock::now();
        auto input=headroom_snapshot(t,0,length);
        bool capacity_ok=true;
        for(int n=0;n<static_cast<int>(input.booked_service.size());++n){
            for(std::size_t i=0;i<batch.size();++i){
                if(!(mask&(1U<<i))||n<o.delay||n>=o.delay+batch[i].term)continue;
                const Count quota=static_cast<Count>(o.capacity)*batch[i].units/N;
                if(quota>input.capacity-input.booked_service[n]){capacity_ok=false;break;}
                input.booked_service[n]+=quota;
            }
            if(!capacity_ok)break;
        }
        bool pass=false;
        if(capacity_ok){
            const auto decision=deadline_headroom_gate(input);
            count["luu_joint_aggregate_gate_interval_checks"]+=decision.interval_checks;
            pass=decision.pass;
        }
        ++count[pass?"luu_joint_aggregate_gate_passed":"luu_joint_aggregate_gate_rejected"];
        gate_times.push_back(std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-started).count());
        return pass;
    }

    BatchPlan luu_batch_plan(int t,const std::vector<TenantProposal>&batch,unsigned mask,const TimedPlanExecutor&executor){
        BatchPlan answer;
        if(mask==0)return answer;
        if(!gate_luu_subset(t,batch,mask))return answer;
        int max_term=0;for(std::size_t i=0;i<batch.size();++i)if(mask&(1U<<i))max_term=std::max(max_term,batch[i].term);
        const Snapshot s=snapshot(t,executor,max_term);
        const int H=std::max(required_horizon(s.delay+max_term,s.envelopes,s.pending),static_cast<int>(s.old_units.size()));
        if(s.old_units.size()<static_cast<std::size_t>(H))throw std::runtime_error("online Luu adapter requires complete old calendar");
        // An already pending atomic replacement is immutable.  Luu-style
        // joint admission may still book into its projected calendar, but it
        // must not overwrite it with another same-epoch global replacement.
        if(executor.has_pending()){
            bool resource_ok=true;std::vector<long double> service(H);
            for(int n=0;n<H;++n){
                int batch_units=0;
                for(std::size_t i=0;i<batch.size();++i)if((mask&(1U<<i))&&n>=o.delay&&n<o.delay+batch[i].term)batch_units+=batch[i].units;
                ++answer.resource_checks;
                if(s.old_units[n]+32+booked(t+n)+batch_units>N){resource_ok=false;break;}
                service[n]=floor_service(s,s.old_units[n]);
            }
            if(resource_ok){++answer.certificate_calls;
                if(plan_certificate(s.envelopes,s.pending,service,s.old_final_change,service.back()).feasible){answer.feasible=true;answer.target=-1;answer.term=0;}}
            return answer;
        }
        // A finite booking may safely use a transient floor below the
        // sustainable tail B.  Searching the full legal range BASE..MAXIMUM
        // matches the shared request-planner action space.
        for(int target=BASE;target<=MAXIMUM;++target){
            bool resource_ok=true;std::vector<long double> service(H);
            for(int n=0;n<H;++n){
                int batch_units=0;
                for(std::size_t i=0;i<batch.size();++i)if((mask&(1U<<i))&&n>=o.delay&&n<o.delay+batch[i].term)batch_units+=batch[i].units;
                const int u=n<o.delay?s.old_units[n]:(n<o.delay+max_term?target:B);
                ++answer.resource_checks;
                if(u+32+booked(t+n)+batch_units>N){resource_ok=false;break;}
                service[n]=floor_service(s,u);
            }
            if(!resource_ok)continue;
            ++answer.certificate_calls;
            if(plan_certificate(s.envelopes,s.pending,service,s.delay+max_term,floor_service(s,B)).feasible){
                answer.feasible=true;answer.target=target;answer.term=max_term;return answer;
            }
        }
        return answer;
    }


    struct LuuSelection { unsigned mask; BatchPlan plan; };
    template<class Planner>
    LuuSelection select_luu_subset(const std::vector<TenantProposal>&candidates,Planner planner){
        unsigned best_mask=0;BatchPlan best;long long best_score=-1;int best_count=-1;
        for(unsigned mask=1;mask<(1U<<candidates.size());++mask){
            ++count["luu_joint_subset_evaluations"];const BatchPlan candidate=planner(mask);
            count["luu_joint_certificate_calls"]+=candidate.certificate_calls;count["luu_joint_resource_checks"]+=candidate.resource_checks;
            if(!candidate.feasible)continue;
            long long score=0;int selected=0;for(std::size_t i=0;i<candidates.size();++i)if(mask&(1U<<i)){score+=candidates[i].priority;++selected;}
            if(score>best_score||(score==best_score&&selected>best_count)||(score==best_score&&selected==best_count&&candidate.target<best.target)){
                best_mask=mask;best=candidate;best_score=score;best_count=selected;
            }
        }
        return {best_mask,best};
    }

    std::vector<std::size_t> admit_luu_joint_batch(int t,const std::vector<TenantProposal>&offers,TimedPlanExecutor&executor){
        ++count["luu_joint_batches"];
        std::vector<TenantProposal> candidates;std::vector<std::size_t> record_indices;
        for(const auto&offer:offers){
            const int start=t+o.delay,end=start+offer.term;const Count work=static_cast<Count>(o.capacity)*offer.units/N*offer.term;
            lease_offers.push_back({t,start,end,offer.units,work});auto&record=lease_offers.back();record.priority=offer.priority;
            record_indices.push_back(lease_offers.size()-1);++count["lease_offered"];count["lease_offered_unit_ms"]+=static_cast<Count>(offer.units)*offer.term;count["lease_offered_work_bytes"]+=work;
            bool allowed=true;
            if(o.gate_eta>=0){record.gate_tested=true;record.gate_passed=gate_request(t,offer.units,offer.term);allowed=record.gate_passed;}
            if(allowed)candidates.push_back(offer);else {++count["lease_rejected"];++count["lease_admission_conflicts"];}
        }
        const auto selection=select_luu_subset(candidates,[&](unsigned mask){return luu_batch_plan(t,candidates,mask,executor);});
        const unsigned best_mask=selection.mask;const BatchPlan best=selection.plan;
        std::vector<std::size_t> accepted;
        bool installed=false;
        if(best_mask){
            installed=best.term==0?true:executor.propose(t,o.delay,best.target,best.term,B,true);
            if(installed){if(best.term>0)++count["luu_joint_plan_commands"];executor_for_commit=&executor;
                for(std::size_t i=0;i<candidates.size();++i)if(best_mask&(1U<<i)){
                    const auto&offer=candidates[i];const std::size_t original=static_cast<std::size_t>(offer.ordinal);
                    const std::size_t record_index=record_indices.at(original);auto&record=lease_offers.at(record_index);
                    append_accepted_lease(t,record.start,record.end,offer.units,record.work,record);record.joint=true;accepted.push_back(record.index);
                }
                executor_for_commit=nullptr;
            }
        }
        for(std::size_t original=0;original<offers.size();++original){
            const std::size_t record_index=record_indices.at(original);auto&record=lease_offers.at(record_index);
            bool selected=false;
            for(std::size_t i=0;i<candidates.size();++i)if(candidates[i].ordinal==static_cast<int>(original)&&(best_mask&(1U<<i)))selected=installed;
            // A gate failure was recorded at offer creation.  Do not count it
            // again merely because it cannot appear in the selected subset.
            if(!selected&&!record.accepted&&!(record.gate_tested&&!record.gate_passed)){++count["lease_rejected"];++count["lease_admission_conflicts"];}
        }
        return accepted;
    }

    bool admit_request(int t,int units,int L,TimedPlanExecutor&executor){
        const int start=t+o.delay,end=start+L;
        const Count work=static_cast<Count>(o.capacity)*units/N*L;
        lease_offers.push_back({t,start,end,units,work});auto&record=lease_offers.back();
        ++count["lease_offered"];count["lease_offered_unit_ms"]+=static_cast<Count>(units)*L;count["lease_offered_work_bytes"]+=work;
        // Every offer passes this same gate before ordinary or joint admission.
        // A rejection preserves the old calendar, bookings and offered-work denominator.
        if(o.gate_eta>=0){record.gate_tested=true;record.gate_passed=gate_request(t,units,L);
            if(!record.gate_passed){++count["lease_rejected"];++count["lease_admission_conflicts"];return false;}}
        bool approved=true;
        for(int tau=start;tau<end;++tau)if(executor.units_at(tau)+32+booked(tau)+units>N){approved=false;break;}
        if(approved&&(o.policy=="joint_guarded"||o.policy=="luu_sequential_style"||o.policy=="yu_gateway_style"))
            approved=online_existing_admission_guard(t,units,L,executor);
        if(o.policy=="residual_priority"&&approved){
            ++count["residual_admission_calls"];
            ++count["planning_calls"];++count["certificate_calls"];
            const auto begun=std::chrono::steady_clock::now();
            const auto snap=snapshot(t,executor,L);
            bool valid=true;for(const auto&p:snap.pending)if(p.deadline<=0)valid=false;
            if(valid){
                const int last=std::max(o.delay+L,std::max(0,latest_lease_end-t));
                const int H=required_horizon(last,contracts,snap.pending);
                std::vector<std::int64_t> old;for(int n=0;n<H;++n)old.push_back(booked_bytes(t+n));
                const auto d=residual_admission_certificate(contracts,snap.pending,o.capacity,o.capacity*27/N+o.capacity*5/N,old,o.delay,L,static_cast<Count>(o.capacity)*units/N,last);
                approved=d.feasible;count["residual_interval_checks"]+=d.checks;
            }else approved=false;
            if(!approved)++count["residual_certificate_rejections"];
            admission_planning_times.push_back(std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-begun).count());
            planning_times.push_back(admission_planning_times.back());
        }
        if(!approved){
            ++count["lease_initial_calendar_conflicts"];
            const bool joint=o.policy=="joint_guarded"||o.policy=="priority_joint"||o.policy=="luu_sequential_style"||o.policy=="yu_gateway_style";
            const bool outage=t>=o.outage_start&&t<static_cast<Count>(o.outage_start)+o.outage_length;
            if(joint&&executor.has_pending())++count["joint_skipped_pending"];
            else if(joint&&outage)++count["joint_skipped_outage"];
            else if(joint){
                ++count["planning_calls"];++count["joint_admission_calls"];
                const auto started=std::chrono::steady_clock::now();
                const Snapshot snap=snapshot(t,executor,L);
                const RequestPlan candidate=o.policy=="yu_gateway_style"?yu_event_request_plan(snap,units,L,B):request_plan(snap,units,L,B);
                count["certificate_calls"]+=candidate.certificate_calls;
                count["joint_certificate_calls"]+=candidate.certificate_calls;
                count["joint_resource_slot_checks"]+=candidate.resource_checks;
                if(candidate.feasible){
                    // Single-threaded event: install/accept the certified
                    // replacement and book its enabling request together.
                    approved=executor.propose(t,o.delay,candidate.target,L,B,true);
                    if(approved){record.joint=true;++count["joint_admission_commands"];++count["certified_approved_commands"];}
                    else ++count["joint_executor_rejections"];
                }else ++count["joint_no_feasible_plan"];
                const auto elapsed=std::chrono::duration<long double,std::micro>(std::chrono::steady_clock::now()-started).count();
                planning_times.push_back(elapsed);admission_planning_times.push_back(elapsed);
            }
        }
        if(approved){
            // Recheck the accepted pending calendar before any work can run.
            for(int tau=start;tau<end;++tau)if(executor.units_at(tau)+32+booked(tau)+units>N)
                throw std::runtime_error("Accepted joint request calendar inconsistent");
            record.accepted=true;record.index=static_cast<int>(leases.size());
            leases.push_back({t,start,end,units,work,work});
            ++count["lease_accepted"];count["lease_accepted_unit_ms"]+=static_cast<Count>(units)*L;count["lease_accepted_work_bytes"]+=work;
            latest_lease_end=std::max(latest_lease_end,end);for(int tau=start;tau<end;++tau){ledger[tau]+=units;tenant_byte_ledger[tau]+=static_cast<Count>(o.capacity)*units/N;}
            if(o.policy=="yu_gateway_style")yu_recompute_minimum_floor(t,executor,"arrival");
        }else {++count["lease_rejected"];++count["lease_admission_conflicts"];}
        return approved;
    }
    void export_leases(){if(o.lease_export.empty())return;std::ofstream f(o.lease_export);if(!f)throw std::runtime_error("Cannot write lease outcomes");
        f<<"offer_ms,start_ms,end_ms,units,work_bytes,accepted,joint_replan,completion_ms,served_bytes,on_time,gate_tested,gate_passed,priority\n";
        for(const auto&r:lease_offers){const Lease*x=r.accepted?&leases[r.index]:nullptr;
            f<<r.offer<<','<<r.start<<','<<r.end<<','<<r.units<<','<<r.work<<','<<r.accepted<<','<<r.joint<<','
             <<(x?x->completion:-1)<<','<<(x?x->work-x->remaining:0)<<','<<(x&&x->completion>=0&&x->completion<=x->end)<<','<<r.gate_tested<<','<<r.gate_passed<<','<<r.priority<<'\n';}
    }
    void finish_outcomes(){
        for(auto&x:outcomes){if(x.status=="queued"||x.status=="ingress_pending"){each(x.p,[](Stats&s){++s.unfinished;});}
            const bool timely=x.completion>=0&&x.completion<=x.p.deadline;
            if(!timely){each(x.p,[](Stats&s){++s.failures;});if(x.p.cls==0){failure_deadlines.push_back(x.p.deadline);if(x.admitted)++critical_admitted_failures;partial_failed_critical_bytes+=x.p.bytes-x.remaining;}}
        }
        queue_end_bytes=std::accumulate(backlog.begin(),backlog.end(),0LL);
    }
    void export_outcomes(){if(o.packet_export.empty())return;std::ofstream f(o.packet_export);if(!f)throw std::runtime_error("Cannot write packet outcomes");
        f<<"arrival_ms,class_id,size_bytes,deadline_ms,source,id,type,queue_arrival_ms,completion_ms,remaining_bytes,status,on_time,queue_admitted\n";
        for(const auto&p:offered){const auto&x=outcomes[p.id];f<<p.arrival<<','<<p.cls<<','<<p.bytes<<','<<p.deadline<<','<<p.source<<','<<p.id<<','<<p.type<<','<<x.queue_arrival<<','<<x.completion<<','<<x.remaining<<','<<x.status<<','<<(x.completion>=0&&x.completion<=p.deadline)<<','<<x.admitted<<'\n';}}
    void execute(){
        std::ofstream output(o.output);if(!output)throw std::runtime_error("Cannot open output JSON");
        if(B<0){Json j(output);j.text("status","configuration_infeasible");j.boolean("configuration_infeasible",true);j.text("reason","No sustainable class-contract tail within maximum floor");j.text("policy",o.policy);j.number("capacity",o.capacity);j.number("contract_factor",o.contract_factor);j.number("gate_eta",o.gate_eta);j.close();return;}
        TimedPlanExecutor executor(initial);IngressShaper shaper(contracts,o.shaping_budget);
        std::mt19937 lease_rng(static_cast<unsigned>(o.seed)^0x85ebca6bU);auto uniform=[&](){return lease_rng()/4294967296.0;};
        std::size_t next=0;std::vector<std::size_t> active_leases;
        const bool priority_dispatch=o.policy=="priority"||o.policy=="priority_guarded"||o.policy=="priority_joint"||o.policy=="residual_priority";
        for(int t=0;t<observation;++t){executor.advance(t);
            if(o.gate_eta>=0)original_history.advance(t);
            if(t>=o.outage_start&&t<static_cast<Count>(o.outage_start)+o.outage_length)++count["planner_outage_slots"];
            while(next<offered.size()&&offered[next].arrival==t){const Packet p=offered[next++];each(p,[&](Stats&s){++s.generated;s.generated_bytes+=p.bytes;});
                if(o.gate_eta>=0)original_history.observe(p.type,p.bytes);
                if(p.cls==0){outcomes[p.id].status="ingress_pending";if(o.gate_eta>=0)known_ingress.insert(p.id);shaper.add(p);}else admit(p,t);}
            const auto step=shaper.advance(t);
            for(const auto&p:step.dropped){known_ingress.erase(p.id);outcomes[p.id].status="ingress_drop";each(p,[](Stats&s){++s.ingress_dropped;});++dropped_shaping_wait_hist[t-p.arrival];}
            for(const auto&p:step.accepted){const int delay=t-p.arrival;each(p,[](Stats&s){++s.shaper_accepted;});
                known_ingress.erase(p.id);
                shape_delay_sum+=delay;shape_delay_max=std::max<Count>(shape_delay_max,delay);++shaping_delay_hist[delay];
                shaped_arrivals[deadline_group(p)][t]+=p.bytes;admit(p,t);}
            while(!queues[0].empty()&&outcomes[queues[0].top()].p.deadline<=t){auto&x=outcomes[queues[0].top()];queues[0].pop();backlog[0]-=x.remaining;x.status="expired";each(x.p,[](Stats&s){++s.expired;});}
            if(o.expire_background)for(int k=1;k<3;++k){while(!queues[k].empty()&&outcomes[queues[k].top()].p.deadline<=t){auto&x=outcomes[queues[k].top()];queues[k].pop();backlog[k]-=x.remaining;x.status="expired";each(x.p,[](Stats&s){++s.expired;});}}
            if(t%o.step==0)plan(t,executor);
            const double request_multiplier=o.family=="synchronized"?(t%2000<500?3.0:1.0/3.0):(o.family=="regime_shift"?(t>=o.duration/2?1.5:.5):1.0);
            // The tenant RNG is independent of controller state.  A fixed
            // five-ms offer epoch and per-request rate make every policy and
            // tuning configuration replay the same seeded offer trace.
            constexpr int V7_OFFER_EPOCH_MS=5;
            if(t<o.duration&&t%V7_OFFER_EPOCH_MS==0&&uniform()<std::min(1.0,o.lease_rate*request_multiplier*V7_OFFER_EPOCH_MS/(1000.0*o.batch_size))){
                std::vector<TenantProposal> batch;batch.reserve(o.batch_size);
                for(int ordinal=0;ordinal<o.batch_size;++ordinal){
                    int units=8*(1+static_cast<int>(uniform()*4));
                    const int terms[3]={5,20,100};int L=o.lease_term?o.lease_term:terms[std::min(2,static_cast<int>(uniform()*3))];
                    if(o.family=="heavy_tenants"){const double z=uniform();units=z<.4?4:(z<.7?8:(z<.85?16:(z<.95?32:48)));L=std::min(300,static_cast<int>(std::ceil(20*std::pow(std::max(1e-9,1-uniform()),-1/1.4))));}
                    else if(o.family=="deadline_mix"){const int sizes[4]={4,12,20,28},lengths[4]={10,40,80,160};units=sizes[std::min(3,static_cast<int>(uniform()*4))];L=lengths[std::min(3,static_cast<int>(uniform()*4))];}
                    batch.push_back({units,L,declared_priority(units,L,ordinal),ordinal});
                }
                // Immutable source trace is recorded before policy-specific
                // sorting, gating, admission, or command effects.
                for(const auto&offer:batch)raw_offer_trace.push_back({t,t+o.delay,t+o.delay+offer.term,offer.units,offer.term,offer.priority,offer.ordinal});
                CompleteTimer complete_batch_timer(complete_batch_times);
                if(o.policy=="luu_joint_style"){
                    const auto indices=admit_luu_joint_batch(t,batch,executor);active_leases.insert(active_leases.end(),indices.begin(),indices.end());
                }else{
                    if(o.policy=="luu_sequential_style"){
                        ++count["luu_sequential_batches"];
                        std::stable_sort(batch.begin(),batch.end(),[](const TenantProposal&a,const TenantProposal&b){return a.priority>b.priority;});
                    }
                    for(const auto&offer:batch){
                        if(admit_request(t,offer.units,offer.term,executor)){lease_offers.back().priority=offer.priority;active_leases.push_back(leases.size()-1);}
                    }
                }
            }
            if(executor.units_at(t)+32+booked(t)>N){++count["capacity_ledger_violations"];throw std::runtime_error("Irrevocable capacity ledger oversubscribed");}
            const int actual=(priority_dispatch?BASE:executor.current_units());
            actual_area+=actual-BASE;standby_area+=executor.max_future_units(t)-BASE;
            undrawn_timed_area+=executor.max_future_units(t)-executor.current_units();
            unconfigured_rights_area+=executor.max_future_units(t)-actual;
            for(int tau=t;tau<t+100;++tau)promised_area100+=executor.committed_at(tau)-BASE;
            int available=o.capacity,used=0;
            for(auto index:active_leases){auto&lease=leases[index];if(t>=lease.start&&t<lease.end){const int due=static_cast<int>(static_cast<Count>(o.capacity)*lease.units/N);const int served=static_cast<int>(std::min<Count>(due,lease.remaining));
                    lease.remaining-=served;available-=served;used+=served;lease_bytes+=served;if(t<o.duration)active_lease_bytes+=served;
                    if(served!=due)++count["lease_floor_deficit_slots"];
                    if(lease.remaining==0&&!lease.completed){lease.completed=true;lease.completion=t+1;++count["lease_completed"];if(t+1<=lease.end)++count["lease_on_time"];}}
            }
            const std::size_t active_before_erase=active_leases.size();
            active_leases.erase(std::remove_if(active_leases.begin(),active_leases.end(),[&](std::size_t i){return leases[i].end<=t+1;}),active_leases.end());
            if(o.policy=="yu_gateway_style"&&active_leases.size()!=active_before_erase)yu_recompute_pending=true;
            const int floors[3]={actual,27,5};for(int k=0;k<3;++k){const int expected=static_cast<int>(std::min<Count>(backlog[k],static_cast<Count>(o.capacity)*floors[k]/N));
                const int served=take(k,expected,t);available-=served;used+=served;if(k>0&&served!=expected)count["background_floor_deficit_bytes"]+=expected-served;}
            if(available<0)throw std::runtime_error("Floor service exceeds physical capacity");
            if(priority_dispatch){const int served=take(0,available,t);available-=served;used+=served;const int bg=spare_weighted(available,t,true);available-=bg;used+=bg;}
            else {const int spare=spare_weighted(available,t);available-=spare;used+=spare;}
            if(used+available!=o.capacity)throw std::runtime_error("Slot byte conservation failed");
            total_used+=used;if(t<o.duration)active_used+=used;
        }
        if(next!=offered.size())throw std::runtime_error("Offered packet enrollment incomplete");
        finish_outcomes();export_outcomes();export_leases();export_raw_offer_trace();
        std::array<Count,3> envelope_excess{},envelope_end_violations{};
        for(int k=0;k<3;++k){Count prefix=0,minimum=0;const Count rho=static_cast<Count>(contracts[k].rho),sigma=static_cast<Count>(contracts[k].sigma);
            for(int b=1;b<=observation;++b){prefix+=shaped_arrivals[k][b-1];const Count transformed=prefix-rho*b;const Count excess=std::max<Count>(0,transformed-minimum-sigma);
                envelope_excess[k]=std::max(envelope_excess[k],excess);if(excess>0)++envelope_end_violations[k];minimum=std::min(minimum,transformed);}}
        Count envelope_violations=std::accumulate(envelope_end_violations.begin(),envelope_end_violations.end(),0LL);
        if(envelope_violations)throw std::runtime_error("Shaper failed its enforced all-interval contract");
        std::array<Count,3> remaining_bytes{};for(const auto&x:outcomes)remaining_bytes[x.p.cls]+=x.remaining;
        for(int k=0;k<3;++k){const auto&s=classes[k];
            if(s.generated!=s.on_time+s.failures||s.completed>s.queue_admitted||s.generated_bytes!=s.served_bytes+remaining_bytes[k])
                throw std::runtime_error("Packet cohort accounting failed");}
        if(total_used!=lease_bytes+classes[0].served_bytes+classes[1].served_bytes+classes[2].served_bytes)
            throw std::runtime_error("Aggregate byte conservation failed");
        if(lease_bytes!=count["lease_accepted_work_bytes"]||count["lease_on_time"]!=count["lease_accepted"])throw std::runtime_error("Accepted lease missed its promised service");
        if(o.gate_eta>=0&&(count["gate_calls"]!=count["lease_offered"]||count["gate_calls"]!=count["gate_passed"]+count["gate_rejected"]))throw std::runtime_error("Gate request accounting failed");
        if(!known_ingress.empty())throw std::runtime_error("Known ingress cohort not drained");
        std::sort(failure_deadlines.begin(),failure_deadlines.end());std::vector<long double> episode_lengths;
        if(!failure_deadlines.empty()){int start=failure_deadlines[0],last=start;for(std::size_t i=1;i<failure_deadlines.size();++i){int t=failure_deadlines[i];if(t-last>5){episode_lengths.push_back(last-start+1);start=t;}last=t;}episode_lengths.push_back(last-start+1);}
        Json j(output);j.text("status","PASS");j.boolean("configuration_infeasible",false);j.text("policy",o.policy);j.text("profile",o.profile);j.text("context",o.context);
        j.text("extension","frozen_joint_retuning_and_workload_confirmation");j.text("workload_family",o.family);j.number("expire_background",o.expire_background);j.text("critical_service_entitlement",o.policy=="residual_priority"?"priority_residual_after_background_and_tenants":"configured_floor");j.number("gate_eta",o.gate_eta);j.number("gate_window_ms",OriginalArrivalWindow::width);
        j.boolean("background_deadline_guarantee",false);j.text("gate_history_interval","[max(0,t-99),t]; fixed100-slot denominator; zero prehistory");
        j.boolean("deadline_certified_policy",o.policy=="guarded"||o.policy=="priority_guarded"||o.policy=="predictive"||o.policy=="joint_guarded"||o.policy=="priority_joint"||o.policy=="residual_priority");j.boolean("initial_plan_certified",initial_plan_certified);
        j.text("admission_start_rule","offer_plus_common_command_delay");j.number("drain_ms",observation-o.duration);j.number("duration_ms",o.duration);j.number("observation_ms",observation);j.number("seed",o.seed);j.number("capacity",o.capacity);j.number("load",o.load);j.number("burst_factor",o.burst_factor);
        j.number("step",o.step);j.number("delay",o.delay);j.number("term",o.term);j.number("price",o.price);j.number("contract_factor",o.contract_factor);j.number("shaping_budget",o.shaping_budget);j.number("guardband",o.guardband);
        j.number("sustainable_tail_units",B);j.number("initial_promised_units",initial);j.number("fixed",o.fixed);j.number("enter",o.enter);j.number("exit",o.exit);j.number("drain_target",o.drain_target);j.number("dwell",o.dwell);
        j.number("hol_upper",o.hol_upper);j.number("hol_lower",o.hol_lower);j.number("lease_rate",o.lease_rate);j.number("lease_term",o.lease_term);j.number("batch_size",o.batch_size);j.number("outage_start",o.outage_start);j.number("outage_length",o.outage_length);
        j.number("actual_extra_unit_ms",actual_area);j.number("reserved_unit_ms",actual_area);j.number("reserve_fraction",static_cast<long double>(actual_area)/N/observation);
        j.number("rolling100_promised_extra_unit_ms",promised_area100);j.number("rolling100_promised_fraction",static_cast<long double>(promised_area100)/100/N/observation);
        j.number("maximum_future_extra_unit_ms",standby_area);j.number("maximum_future_fraction",static_cast<long double>(standby_area)/N/observation);
        j.number("undrawn_timed_rights_unit_ms",undrawn_timed_area);j.number("unconfigured_rights_unit_ms",unconfigured_rights_area);
        j.number("standby_extra_unit_ms",standby_area);j.number("standby_fraction",static_cast<long double>(standby_area)/N/observation);
        j.number("used_bytes",total_used);j.number("capacity_bytes",static_cast<Count>(o.capacity)*observation);j.number("active_used_bytes",active_used);j.number("active_capacity_bytes",static_cast<Count>(o.capacity)*o.duration);
        j.number("active_utilization",static_cast<long double>(active_used)/o.capacity/o.duration);j.number("active_lease_bytes",active_lease_bytes);j.number("lease_served_bytes",lease_bytes);
        j.number("background_active_mbps",8.L*(classes[1].active_served_bytes+classes[2].active_served_bytes)/(1000.L*o.duration));
        j.number("critical_generated",classes[0].generated);j.number("critical_on_time",classes[0].on_time);j.number("critical_failures",classes[0].failures);
        j.number("critical_offered_failure_fraction",static_cast<long double>(classes[0].failures)/std::max<Count>(1,classes[0].generated));
        j.number("critical_queue_admitted",classes[0].queue_admitted);j.number("critical_queue_admitted_failures",critical_admitted_failures);
        j.number("critical_queue_admitted_failure_fraction",static_cast<long double>(critical_admitted_failures)/std::max<Count>(1,classes[0].queue_admitted));
        j.number("shaping_accepted_count",classes[0].shaper_accepted);j.number("shaping_dropped_count",classes[0].ingress_dropped);j.number("shaping_delay_sum_ms",shape_delay_sum);j.number("shaping_delay_max_ms",shape_delay_max);
        j.number("queue_end_bytes",queue_end_bytes);j.number("partial_failed_critical_bytes",partial_failed_critical_bytes);j.number("failure_episode_count",episode_lengths.size());
        j.number("failure_episode_max_ms",episode_lengths.empty()?0:*std::max_element(episode_lengths.begin(),episode_lengths.end()));j.number("failure_episode_p95_ms",quantile(episode_lengths,.95));


        for(const auto&name:{"luu_joint_aggregate_gate_calls","luu_joint_aggregate_gate_passed","luu_joint_aggregate_gate_rejected","luu_joint_aggregate_gate_interval_checks"})j.number(name,count[name]);
        j.number("complete_control_decisions",complete_control_times.size());
        j.number("complete_batch_decisions",complete_batch_times.size());
        j.number("complete_control_time_us_total",std::accumulate(complete_control_times.begin(),complete_control_times.end(),0.L));
        j.number("complete_batch_time_us_total",std::accumulate(complete_batch_times.begin(),complete_batch_times.end(),0.L));
        j.number("complete_control_time_us_p95",quantile(complete_control_times,.95));
        j.number("complete_batch_time_us_p95",quantile(complete_batch_times,.95));
        j.number("complete_control_time_us_max",complete_control_times.empty()?0:*std::max_element(complete_control_times.begin(),complete_control_times.end()));
        j.number("complete_batch_time_us_max",complete_batch_times.empty()?0:*std::max_element(complete_batch_times.begin(),complete_batch_times.end()));
        j.number("planning_time_us_total",std::accumulate(planning_times.begin(),planning_times.end(),0.L));j.number("planning_time_us_p50",quantile(planning_times,.5));j.number("planning_time_us_p95",quantile(planning_times,.95));
        j.number("planning_time_us_max",planning_times.empty()?0:*std::max_element(planning_times.begin(),planning_times.end()));
        j.number("joint_planning_time_us_total",std::accumulate(admission_planning_times.begin(),admission_planning_times.end(),0.L));
        j.number("joint_planning_time_us_p50",quantile(admission_planning_times,.5));j.number("joint_planning_time_us_p95",quantile(admission_planning_times,.95));
        j.number("joint_planning_time_us_max",admission_planning_times.empty()?0:*std::max_element(admission_planning_times.begin(),admission_planning_times.end()));
        const long double gate_total=std::accumulate(gate_times.begin(),gate_times.end(),0.L);
        j.number("gate_time_us_total",gate_total);j.number("gate_time_us_p50",quantile(gate_times,.5));j.number("gate_time_us_p95",quantile(gate_times,.95));j.number("gate_time_us_max",gate_times.empty()?0:*std::max_element(gate_times.begin(),gate_times.end()));
        j.number("controller_compute_time_us_total",gate_total+std::accumulate(planning_times.begin(),planning_times.end(),0.L));
        for(const auto&name:{"residual_admission_calls","residual_interval_checks","residual_certificate_rejections"})j.number(name,count[name]);
        for(const auto&name:{"gate_calls","gate_passed","gate_rejected","gate_interval_checks"})j.number(name,count[name]);
        for(const auto&name:{"regular_planning_calls","joint_admission_calls","joint_certificate_calls","joint_resource_slot_checks","joint_admission_commands","joint_executor_rejections","joint_no_feasible_plan","joint_skipped_pending","joint_skipped_outage","lease_initial_calendar_conflicts","oranus_state_A_observations","oranus_state_B_observations","oranus_state_C_observations","oranus_capped_requests","planner_outage_slots","control_ticks","planning_calls","certificate_calls","no_feasible_plan","guardbudget_exhausted_calls","skipped_pending_ticks","skipped_outage_ticks","dwell_retained_calls","resource_plan_checks","resource_plan_conflicts","certified_decisions","release_decisions","certified_approved_commands","uncertified_approved_commands","release_commands","retained_plan_calls","lease_offered","lease_accepted","lease_rejected","lease_completed","lease_on_time","lease_offered_unit_ms","lease_offered_work_bytes","lease_accepted_unit_ms","lease_accepted_work_bytes","lease_admission_conflicts","capacity_ledger_violations","lease_floor_deficit_slots","background_floor_deficit_bytes","adapter_common_admission_guard_calls","adapter_common_admission_guard_rejections","luu_joint_batches","luu_joint_subset_evaluations","luu_joint_certificate_calls","luu_joint_resource_checks","luu_joint_plan_commands","luu_sequential_batches","yu_event_control_ticks","yu_event_guard_checks","yu_event_guard_rejections","yu_event_commands","yu_event_request_attempts","yu_event_recomputations","yu_arrival_recomputations","yu_expiry_recomputations"})j.number(name,count[name]);
        const auto&c=executor.counters();j.number("plan_commands",c.plan_commands);j.number("accepted_plan_commands",c.accepted_plan_commands);j.number("rejected_commands",c.rejected_commands);j.number("timer_updates",c.timer_updates);
        j.number("actual_floor_changes",priority_dispatch?0:c.actual_floor_changes);j.number("promised_floor_changes",c.actual_floor_changes);j.number("backup_activations",c.backup_activations);j.number("renewals",c.renewals);j.number("superseded_before_expiry",c.superseded_before_expiry);
        j.number("envelope_violation_endpoints",envelope_violations);j.number("initial_certificate_calls",1);
        j.key("contracts");output<<'[';for(int k=0;k<3;++k){if(k)output<<',';Json e(output);e.number("min_deadline",contracts[k].min_deadline);e.number("sigma",contracts[k].sigma);e.number("rho",contracts[k].rho);e.number("maximum_interval_excess_bytes",envelope_excess[k]);e.number("violating_interval_endpoints",envelope_end_violations[k]);e.close();}output<<']';
        auto hist=[&](const char*key,const std::map<int,Count>&h){j.key(key);output<<'{';bool first=true;for(const auto&[d,n]:h){if(!first)output<<',';first=false;output<<quote(std::to_string(d))<<':'<<n;}output<<'}';};
        hist("shaping_delay_histogram_ms",shaping_delay_hist);hist("dropped_shaping_wait_histogram_ms",dropped_shaping_wait_hist);
        j.key("classes");output<<'[';for(int k=0;k<3;++k){if(k)output<<',';stats_json(output,classes[k]);}output<<']';
        j.key("types");output<<'[';for(int k=0;k<7;++k){if(k)output<<',';stats_json(output,types[k]);}output<<']';
        j.key("sources");output<<'[';for(std::size_t k=0;k<sources.size();++k){if(k)output<<',';stats_json(output,sources[k]);}output<<']';j.close();output<<'\n';
    }
};
}
int main(int argc,char**argv){try{Options options=parse(argc,argv);Simulation simulation(options);simulation.execute();}
    catch(const std::exception&e){std::cerr<<"ERROR: "<<e.what()<<'\n';return 2;}return 0;}
