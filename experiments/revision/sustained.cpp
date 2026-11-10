// Sustained byte-service histories using the released production GAR guard.
// This executable does not claim to emulate a radio or certify Linux service.
#include "../online/request_admission.hpp"
#include "../online/plan_executor.hpp"
#include "../gateway/background_admission_gate.hpp"
#include <array>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <queue>
#include <random>
#include <set>
#include <string>
using namespace guarded_release;
using I=std::int64_t;
struct Opt {
 std::string output,policy="gar",family="deadline_mix";
 int duration=300000,seed=800001,capacity=650,units=106,delay=2,step=5,term=40,window=0,background=1,outage=-1,outage_length=0,trace=0;
 double load=.8,rate=10,burst=1,service=1,actual=1,eta=1,price=64;
};
Opt parse(int argc,char**argv){
 Opt o;for(int i=1;i<argc;++i){std::string k=argv[i];if(i+1==argc)throw std::runtime_error("Missing value");std::string v=argv[++i];
 if(k=="--output")o.output=v;else if(k=="--policy")o.policy=v;else if(k=="--family")o.family=v;
 else if(k=="--duration")o.duration=std::stoi(v);else if(k=="--seed")o.seed=std::stoi(v);
 else if(k=="--capacity")o.capacity=std::stoi(v);else if(k=="--units")o.units=std::stoi(v);
 else if(k=="--delay")o.delay=std::stoi(v);else if(k=="--step")o.step=std::stoi(v);else if(k=="--term")o.term=std::stoi(v);
 else if(k=="--window")o.window=std::stoi(v);else if(k=="--background-contract")o.background=std::stoi(v);
 else if(k=="--outage-start")o.outage=std::stoi(v);else if(k=="--outage-length")o.outage_length=std::stoi(v);
 else if(k=="--trace")o.trace=std::stoi(v);else if(k=="--load")o.load=std::stod(v);else if(k=="--rate")o.rate=std::stod(v);
 else if(k=="--burst")o.burst=std::stod(v);else if(k=="--service")o.service=std::stod(v);
 else if(k=="--actual-service")o.actual=std::stod(v);else if(k=="--eta")o.eta=std::stod(v);
 else if(k=="--price")o.price=std::stod(v);else throw std::runtime_error("Unknown option: "+k);}
 if(o.output.empty()||!std::set<std::string>{"peak","keep_calendar","minimum_floor","gar","gar_h"}.count(o.policy)||
 !std::set<std::string>{"deadline_mix","heavy_tenants","regime_shift","synchronized"}.count(o.family))throw std::runtime_error("Unknown method or family");
 if(o.duration<1||o.duration>3600000||o.seed<0||o.capacity<1||o.capacity>10000000||o.units<16||o.units>1000||o.delay<0||o.delay>1000||o.step<1||o.step>1000||o.term<1||o.term>5000||o.window<0||o.window>5000||o.outage_length<0||(o.background!=0&&o.background!=1))throw std::runtime_error("Invalid integer option");
 for(double x:{o.load,o.rate,o.burst,o.service,o.actual,o.eta,o.price})if(!std::isfinite(x))throw std::runtime_error("Nonfinite option");
 if(o.load<=0||o.load>4||o.rate<0||o.rate>1000||o.burst<=0||o.burst>4||o.service<=0||o.service>1||o.actual<=0||o.actual>1||o.eta<0||o.price<0)throw std::runtime_error("Invalid real option");
 return o;
}
struct Job{int at,deadline,cls,id;I work,remaining;int completion=-1;};
struct Offer{int at,start,end,units;I work;int pattern;};
struct Tenant{Offer offer;I remaining;int completion=-1;};
struct Order{const std::vector<Job>*v=nullptr;bool operator()(int a,int b)const{return (*v)[a].deadline==(*v)[b].deadline?a>b:(*v)[a].deadline>(*v)[b].deadline;}};
struct Stats{I offered=0,timely=0,timely_bytes=0,failed=0;};
struct Study{
 Opt o;int cap,base,maximum,tail=-1,stop,last_end=0;std::vector<Envelope> env;std::vector<Job> jobs;std::vector<Offer> offers;std::vector<Tenant> tenants;
 std::vector<int> active,ledger;std::vector<I> byte_ledger;std::array<int,2> bg{};std::array<Stats,6> stats{};
 std::array<std::priority_queue<int,std::vector<int>,Order>,6> queues;TimedPlanExecutor executor{0};OriginalArrivalWindow history;
 std::vector<std::pair<int,int>> changes;std::map<std::string,I> rejects;std::vector<double> times;
 std::vector<I> segment_offers,segment_accepted,segment_timely;
 std::uint64_t fingerprint=14695981039346656037ULL;
 I accepted=0,tenant_done=0,tenant_bytes=0,used=0,critical_area=0,booked_area=0,capacity_violations=0,supply_shortfalls=0,envelope_violations=0;
 explicit Study(Opt x):o(x){
 cap=static_cast<int>(std::floor(o.capacity*o.service));base=std::max(1,static_cast<int>(std::ceil(21.0*o.units/106)));
 stop=o.duration+5001+o.delay+115;ledger.assign(stop+6000,0);byte_ledger.assign(ledger.size(),0);
 segment_offers.assign((o.duration+29999)/30000,0);segment_accepted=segment_offers;segment_timely=segment_offers;
 int D[3]={o.family=="deadline_mix"?10:15,o.family=="deadline_mix"?55:45,o.family=="deadline_mix"?115:95},S[3]={480,3328,2560},R[3]={94,104,100};
 for(int k=0;k<3;++k)env.push_back({D[k],std::ceil(S[k]*o.burst),std::ceil(R[k]*o.load)});
 generate();
 int W[2]={static_cast<int>(std::ceil(2400*o.load)),100};
 for(int k=0;k<2;++k){bg[k]=o.units+1;for(int u=0;u<=o.units;++u)if(static_cast<I>(cap)*u/o.units*10>=W[k]){bg[k]=u;break;}}
 if(!o.background){bg[0]=static_cast<int>(std::ceil(27.0*o.units/106));bg[1]=static_cast<int>(std::ceil(5.0*o.units/106));}
 maximum=std::min(static_cast<int>(std::floor(74.0*o.units/106)),o.units-bg[0]-bg[1]);
 if(cap<1||maximum<base)return;
 Snapshot s;s.capacity=cap;s.units=o.units;s.base=base;s.maximum=maximum;s.envelopes=env;tail=minimum_tail(s);if(tail<0)return;
 executor=TimedPlanExecutor(tail);for(auto&q:queues)q=std::priority_queue<int,std::vector<int>,Order>(Order{&jobs});
 std::array<std::vector<I>,3>a;for(auto&v:a)v.assign(o.duration,0);for(auto&j:jobs)if(j.cls<3)a[j.cls][j.at]+=j.work;
 for(int k=0;k<3;++k){I sum=0,low=0;for(int t=0;t<o.duration;++t){sum+=a[k][t];I z=sum-static_cast<I>(env[k].rho)*(t+1);if(z-low>env[k].sigma)++envelope_violations;low=std::min(low,z);}}
 }
 void hash(I v){for(int b=0;b<8;++b){fingerprint^=static_cast<unsigned char>((static_cast<std::uint64_t>(v)>>(8*b))&255);fingerprint*=1099511628211ULL;}}
 void add(int t,int k,I bytes,int d){int id=static_cast<int>(jobs.size());jobs.push_back({t,t+d,k,id,bytes,bytes,-1});}
 void generate(){
 std::mt19937 rng(o.seed),rq(o.seed^0x51a7e123U);auto random=[&](){return rng()/4294967296.0;};auto request=[&](){return rq()/4294967296.0;};
 std::array<I,3>tokens{};for(int k=0;k<3;++k)tokens[k]=static_cast<I>(env[k].sigma+env[k].rho);
 int video=static_cast<int>(std::ceil(2400*o.load));
 for(int t=0;t<o.duration;++t){
 bool hot=o.family=="synchronized"?t%2000<400:o.family=="regime_shift"?t>=o.duration/2:t%1000<100;
 for(int k=0;k<3;++k){I limit=static_cast<I>(env[k].sigma+env[k].rho);if(t)tokens[k]=std::min(limit,tokens[k]+static_cast<I>(env[k].rho));
 if(random()<(hot?.30:.06)&&tokens[k]>0){I n=std::min(tokens[k],std::max<I>(1,static_cast<I>((hot?.65:.18)*limit*(.6+.4*random()))));tokens[k]-=n;add(t,k,n,env[k].min_deadline);}}
 if(t%10==0){add(t,3,std::max(1,static_cast<int>(video*(.85+.15*random()))),15);add(t,4,85+static_cast<int>(16*random()),20);}
 if(t%5==0)add(t,5,1500+static_cast<int>(1001*random()),50);
 double mult=o.family=="regime_shift"?(t>=o.duration/2?1.5:.5):o.family=="synchronized"?(hot?2.5:.625):1;
 if(t%5==0&&request()<std::min(1.0,o.rate*mult*5/1000)){
 int Q[4]={4,8,16,24},L[4]={20,100,500,2000};
 int qi=o.family=="heavy_tenants"?2+std::min(1,static_cast<int>(request()*2)):std::min(3,static_cast<int>(request()*4));
 int li=o.family=="heavy_tenants"?2+std::min(1,static_cast<int>(request()*2)):std::min(3,static_cast<int>(request()*4));
 int q=std::max(1,static_cast<int>(std::ceil(Q[qi]*o.units/106.0))),length=o.window?o.window:L[li],fractions[3]={65,85,100};
 I work=static_cast<I>(o.capacity)*q/o.units*length*fractions[std::min(2,static_cast<int>(request()*3))]/100;
 offers.push_back({t,t+o.delay,t+o.delay+length,q,work,static_cast<int>(request()*3)});
 }}
 for(auto&j:jobs){hash(j.at);hash(j.deadline);hash(j.cls);hash(j.work);}for(auto&r:offers){hash(r.at);hash(r.start);hash(r.end);hash(r.units);hash(r.work);hash(r.pattern);}
 }
 I quota(int q)const{return static_cast<I>(cap)*q/o.units;}
 int other(int t)const{return bg[0]+bg[1]+(t<static_cast<int>(ledger.size())?ledger[t]:0);}
 bool outage(int t)const{return o.outage>=0&&t>=o.outage&&t<static_cast<I>(o.outage)+o.outage_length;}
 int second(const Offer&r)const{return r.pattern==1?(r.end-r.start)/2:(r.end-r.start)*9/10;}
 bool releases(const Offer&r)const{return r.work>0&&r.work<=quota(r.units)*(r.end-r.start)&&(!r.pattern||r.work-r.work/2<=quota(r.units)*(r.end-r.start-second(r)));}
 Snapshot snapshot(int t,int length=0){
 Snapshot s;s.now=t;s.capacity=cap;s.units=o.units;s.base=base;s.maximum=maximum;s.step=o.step;s.delay=o.delay;s.term=o.term;s.command_price=o.price;s.envelopes=env;
 if(auto e=executor.active_expiry())s.old_final_change=static_cast<int>(*e-t);
 for(int k=0;k<3;++k){auto q=queues[k];while(!q.empty()){auto&j=jobs[q.top()];q.pop();s.pending.push_back({j.deadline-t,static_cast<long double>(j.remaining)});}}
 int T=std::max({s.old_final_change,o.delay+std::max(o.term,length),std::max(0,last_end-t)}),H=required_horizon(T,env,s.pending);
 for(int n=0;n<H;++n){s.old_units.push_back(executor.units_at(t+n));s.other_units.push_back(other(t+n));}return s;
 }
 bool headroom(int t,const Offer&r){
 if(o.policy!="gar_h")return true;HeadroomInput s;s.capacity=cap;s.request_offset=o.delay;s.request_term=r.end-r.start;s.request_bytes_per_slot=quota(r.units);
 auto rates=history.rates(o.eta);for(int k=0;k<5;++k)s.forecast.push_back({k<3?env[k].min_deadline:k==3?15:20,0,rates[k]});
 std::stable_sort(s.forecast.begin(),s.forecast.end(),[](auto&a,auto&b){return a.min_deadline<b.min_deadline;});
 for(int k=0;k<5;++k){auto q=queues[k];while(!q.empty()){auto&j=jobs[q.top()];q.pop();s.pending.push_back({j.deadline-t,static_cast<long double>(j.remaining)});}}
 int H=std::max(required_horizon(o.delay+s.request_term,s.forecast,s.pending),std::max(0,last_end-t)+116);
 for(int n=0;n<H;++n)s.booked_service.push_back(t+n<static_cast<int>(byte_ledger.size())?byte_ledger[t+n]:0);
 return deadline_headroom_gate(s).pass;
 }
 bool accept(const Offer&r,int t){
 auto begin=std::chrono::steady_clock::now();struct Timer{std::vector<double>&v;decltype(begin)b;~Timer(){v.push_back(std::chrono::duration<double,std::micro>(std::chrono::steady_clock::now()-b).count());}}timer{times,begin};
 ++segment_offers[t/30000];if(!releases(r)){++rejects["release_availability"];return false;}if(outage(t)){++rejects["controller_unavailable"];return false;}
 if(!headroom(t,r)){++rejects["causal_headroom"];return false;}
 bool fit=true;for(int n=r.start;n<r.end;++n){int u=o.policy=="peak"?std::max(tail,executor.max_future_units(n)):executor.units_at(n);if(u+other(n)+r.units>o.units){fit=false;break;}}
 if(!fit&&(o.policy=="peak"||o.policy=="keep_calendar")){++rejects["installed_calendar_capacity"];return false;}
 if(!fit){if(executor.has_pending()){++rejects["pending_replacement"];return false;}auto s=snapshot(t,r.end-r.start);auto d=request_plan(s,r.units,r.end-r.start,tail);
 if(!d.feasible){++rejects[d.reason];return false;}if(!executor.propose(t,o.delay,d.target,d.term,d.tail,true))throw std::runtime_error("Serialized proposal failed");}
 for(int n=r.start;n<r.end;++n){ledger[n]+=r.units;byte_ledger[n]+=quota(r.units);}
 active.push_back(static_cast<int>(tenants.size()));tenants.push_back({r,r.work,-1});last_end=std::max(last_end,r.end);++accepted;++segment_accepted[t/30000];return true;
 }
 I take(int k,I amount,int t){
 I used=0;auto&q=queues[k];while(amount>0&&!q.empty()){auto&j=jobs[q.top()];I n=std::min(amount,j.remaining);j.remaining-=n;amount-=n;used+=n;
 if(j.remaining==0){j.completion=t+1;if(j.completion<=j.deadline){++stats[k].timely;stats[k].timely_bytes+=j.work;}q.pop();}}return used;
 }
 I critical(I amount,int t){
 I used=0;while(amount>0){int k=-1;for(int a=0;a<3;++a)if(!queues[a].empty()&&(k<0||jobs[queues[a].top()].deadline<jobs[queues[k].top()].deadline))k=a;
 if(k<0)break;I n=take(k,std::min(amount,jobs[queues[k].top()].remaining),t);used+=n;amount-=n;}return used;
 }
 I released(const Tenant&x,int t)const{if(t<x.offer.start)return 0;if(!x.offer.pattern||t>=x.offer.start+second(x.offer))return x.offer.work;return x.offer.work/2;}
 I tenant(int id,I amount,int t){
 auto&x=tenants[id];I n=std::min(amount,std::max<I>(0,released(x,t)-(x.offer.work-x.remaining)));x.remaining-=n;
 if(!x.remaining&&x.completion<0){x.completion=t+1;if(x.completion<=x.offer.end){++tenant_done;tenant_bytes+=x.offer.work;++segment_timely[x.offer.at/30000];}}return n;
 }
 void run(){
 if(tail<0){report();return;}std::size_t ji=0,ri=0;
 for(int t=0;t<stop;++t){
 executor.advance(t);history.advance(t);while(ji<jobs.size()&&jobs[ji].at==t){auto&j=jobs[ji];++stats[j.cls].offered;queues[j.cls].push(static_cast<int>(ji));if(j.cls<5)history.observe(j.cls,j.work);++ji;}
 while(ri<offers.size()&&offers[ri].at==t){accept(offers[ri],t);++ri;}
 if((o.policy=="gar"||o.policy=="gar_h")&&t<o.duration&&t%o.step==0&&!executor.has_pending()&&!outage(t)){auto s=snapshot(t);auto d=guarded_decision(s);if(d.certified&&d.replace&&!executor.propose(t,o.delay,d.target,d.term,d.tail,true))throw std::runtime_error("Control proposal failed");}
 int u=executor.current_units();if(changes.empty()||changes.back().second!=u)changes.push_back({t,u});critical_area+=u;booked_area+=ledger[t];if(u+other(t)>o.units)++capacity_violations;
 int actual=t>=o.duration/2?static_cast<int>(std::floor(o.capacity*o.actual)):o.capacity;if(actual<cap)++supply_shortfalls;I available=actual,spent=0;
 auto budget=[&](int q){return std::min(available,static_cast<I>(actual)*q/o.units);};
 I n=critical(budget(u),t);available-=n;spent+=n;for(int k=0;k<2;++k){n=take(k+3,budget(bg[k]),t);available-=n;spent+=n;}
 for(int id:active)if(t>=tenants[id].offer.start&&t<tenants[id].offer.end){n=tenant(id,budget(tenants[id].offer.units),t);available-=n;spent+=n;}
 while(available>0){int k=-1,id=-1,d=std::numeric_limits<int>::max();for(int a=0;a<6;++a)if(!queues[a].empty()&&jobs[queues[a].top()].deadline<d){k=a;d=jobs[queues[a].top()].deadline;}
 for(int x:active)if(t>=tenants[x].offer.start&&t<tenants[x].offer.end&&released(tenants[x],t)>tenants[x].offer.work-tenants[x].remaining&&tenants[x].offer.end<d){k=-1;id=x;d=tenants[x].offer.end;}
 if(k<0&&id<0)break;n=k>=0?take(k,std::min(available,jobs[queues[k].top()].remaining),t):tenant(id,available,t);if(!n)throw std::runtime_error("EDF made no progress");available-=n;spent+=n;}
 if(spent>actual)throw std::runtime_error("Physical capacity exceeded");used+=spent;
 for(int k=0;k<6;++k)while(!queues[k].empty()&&jobs[queues[k].top()].deadline<=t+1){auto&j=jobs[queues[k].top()];if(j.remaining)++stats[k].failed;queues[k].pop();}
 active.erase(std::remove_if(active.begin(),active.end(),[&](int id){return tenants[id].offer.end<=t+1;}),active.end());
 }
 for(auto&q:queues)if(!q.empty())throw std::runtime_error("Incomplete drain");if(!active.empty())throw std::runtime_error("Undrained bookings");report();
 }
 void report(){
 std::ofstream f(o.output);if(!f)throw std::runtime_error("Cannot open output");f<<std::setprecision(17)<<"{\n";
 auto text=[&](std::string k,std::string v){f<<std::quoted(k)<<":"<<std::quoted(v)<<",\n";};auto number=[&](std::string k,auto v){f<<std::quoted(k)<<":"<<v<<",\n";};
 text("status",tail<0?"configuration_infeasible":"COMPLETE");text("policy",o.policy);text("family",o.family);text("model","byte_preemptive_gateway_production_GAR_certificate");
 number("seed",o.seed);number("duration_ms",o.duration);number("execution_ms",stop);number("capacity",o.capacity);number("certified_capacity",cap);number("units",o.units);number("tail_units",tail);number("background_units",bg[0]+bg[1]);number("protected_background",o.background);
 number("input_fingerprint_fnv64",fingerprint);number("offered_requests",offers.size());number("accepted_requests",accepted);number("timely_tenants",tenant_done);number("timely_tenant_bytes",tenant_bytes);
 I timely=tenant_bytes,failed=0,offered=0;for(int k=0;k<6;++k){timely+=stats[k].timely_bytes;if(k<3){failed+=stats[k].failed;offered+=stats[k].offered;}}
 number("critical_offered",offered);number("critical_failed",failed);number("background_offered",stats[3].offered+stats[4].offered);number("background_failed",stats[3].failed+stats[4].failed);number("best_effort_failed",stats[5].failed);
 number("capacity_ledger_violations",capacity_violations);number("envelope_violations",envelope_violations);number("supply_shortfall_slots",supply_shortfalls);
 number("timely_bytes",timely);number("capacity_bytes",static_cast<I>(o.capacity)*stop);number("served_bytes",used);number("critical_unit_ms",critical_area);number("booked_unit_ms",booked_area);number("plan_commands",executor.counters().accepted_plan_commands);
 auto timing=times;std::sort(timing.begin(),timing.end());number("decision_us_p95",timing.empty()?0:timing[static_cast<std::size_t>(std::ceil(.95*timing.size()))-1]);number("decision_us_max",timing.empty()?0:timing.back());
 f<<"\"segments\":[";for(std::size_t n=0;n<segment_offers.size();++n){if(n)f<<',';f<<"{\"start_ms\":"<<n*30000<<",\"offered\":"<<segment_offers[n]<<",\"accepted\":"<<segment_accepted[n]<<",\"timely\":"<<segment_timely[n]<<'}';}
 f<<"],\n\"rejections\":{";bool first=true;for(auto&[k,v]:rejects){if(!first)f<<',';first=false;f<<std::quoted(k)<<':'<<v;}f<<"}\n}\n";
 if(tail>=0){
 std::ofstream tr(o.output+".tenants.csv");tr<<"offer_ms,start_ms,end_ms,units,work_bytes,remaining_bytes,completion_ms,release_pattern\n";for(auto&x:tenants)tr<<x.offer.at<<','<<x.offer.start<<','<<x.offer.end<<','<<x.offer.units<<','<<x.offer.work<<','<<x.remaining<<','<<x.completion<<','<<x.offer.pattern<<'\n';
 std::ofstream cal(o.output+".calendar.csv");cal<<"time_ms,critical_units\n";for(auto&[t,u]:changes)cal<<t<<','<<u<<'\n';
 if(o.trace){std::ofstream j(o.output+".jobs.csv");j<<"id,arrival_ms,deadline_ms,class_id,work_bytes,remaining_bytes,completion_ms\n";for(auto&x:jobs)j<<x.id<<','<<x.at<<','<<x.deadline<<','<<x.cls<<','<<x.work<<','<<x.remaining<<','<<x.completion<<'\n';}
 }}
};
int main(int argc,char**argv){try{Study s(parse(argc,argv));s.run();}catch(const std::exception&e){std::cerr<<"ERROR: "<<e.what()<<'\n';return 2;}return 0;}
