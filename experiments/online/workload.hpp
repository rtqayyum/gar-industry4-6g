#ifndef GUARDED_RELEASE_WORKLOAD_HPP
#define GUARDED_RELEASE_WORKLOAD_HPP
#include "deadline_certificate.hpp"
#include <algorithm>
#include <array>
#include <cmath>
#include <deque>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>
namespace guarded_release {
struct Packet {
    int arrival,cls,bytes,deadline,source,type,id,remaining,queue_arrival;
};
struct WorkloadOptions {
    int seed=1001,duration=10000,endpoints=100;
    double load=1.0,burst_factor=2.5;
    std::string profile="industrial",family="original";
};
inline std::vector<double> csv_values(const std::string&line) {
    std::stringstream stream(line);std::string item;std::vector<double> values;
    while(std::getline(stream,item,','))values.push_back(std::stod(item));
    return values;
}
inline std::array<int,7> source_counts(int endpoints) {
    if(endpoints<1)throw std::invalid_argument("Positive endpoint count required");
    const int counts[7]={5,10,10,55,16,2,2};std::array<int,7> n{};
    for(int k=0;k<7;++k)n[k]=std::max(1,static_cast<int>(std::round(static_cast<double>(counts[k])*endpoints/100.0)));
    return n;
}
inline std::array<int,3> critical_deadlines(const std::string&profile) {
    if(profile=="industrial")return {20,50,100};
    if(profile=="urllc")return {5,10,20};
    if(profile=="mixed")return {15,60,120};
    throw std::invalid_argument("Unknown deadline profile");
}
inline int deadline_group(const Packet&p) {
    return p.type==0?0:((p.type==1||p.type==5)?1:2);
}
inline std::vector<Packet> generate_packets(const std::string&path,const WorkloadOptions&o) {
    if(o.duration<1||!std::isfinite(o.load)||!std::isfinite(o.burst_factor)||o.load<=0||o.burst_factor<=0)throw std::invalid_argument("Invalid workload options");
    std::ifstream f(path);if(!f)throw std::runtime_error("Cannot read context CSV");
    std::string line;std::getline(f,line);std::vector<std::array<double,3>> context;
    while(std::getline(f,line)){auto v=csv_values(line);if(v.size()!=3)throw std::invalid_argument("Three context scales required");for(double x:v)if(!std::isfinite(x)||x<=0||x>1.4)throw std::invalid_argument("Context scales must be finite and in (0,1.4]");context.push_back({v[0],v[1],v[2]});}
    if(context.empty())throw std::invalid_argument("Empty context");
    const int cls[7]={0,0,0,2,2,0,1},bytes[7]={96,128,256,128,256,128,1200},period[7]={20,50,100,1000,200,2000,10};
    auto counts=source_counts(o.endpoints);auto cd=critical_deadlines(o.profile);
    const int D[7]={cd[0],cd[1],cd[2],o.family=="deadline_mix"?150:500,o.family=="deadline_mix"?80:200,cd[1],o.family=="deadline_mix"?50:100};
    std::mt19937 rng(o.seed);auto unit=[&](){return static_cast<double>(rng())/4294967296.0;};
    const int random_phase=static_cast<int>(unit()*4000);
    const int phase=o.family=="synchronized"?0:random_phase;int id=0,source=0;std::vector<Packet> packets;
    for(int k=0;k<7;++k)for(int j=0;j<counts[k];++j,++source){
        double next=k==5?0:unit()*period[k];
        while(next<o.duration){const int t=static_cast<int>(std::floor(next));const auto c=context[std::min(static_cast<int>(context.size())-1,t/1000)];
            double scale=cls[k]==0?c[0]:(cls[k]==1?c[1]:c[2]);
            const int cycle=o.family=="synchronized"?2000:(o.family=="heavy_tenants"?1000:(o.family=="regime_shift"?8000:4000));
            const double hot=cls[k]==0&&(t+phase)%cycle<cycle/10?o.burst_factor:1;
            if(cls[k]!=0&&o.family=="synchronized")scale*=t%2000<500?3.0:1.0/3.0;
            for(int b=0;b<(k==5?8:1);++b)packets.push_back({t,cls[k],bytes[k],t+D[k],source,k,id++,bytes[k],t});
            next+=std::max(.1,period[k]/(scale*o.load*hot)*(.9+.2*unit()));
        }
    }
    std::stable_sort(packets.begin(),packets.end(),[](const Packet&a,const Packet&b){return a.arrival!=b.arrival?a.arrival<b.arrival:a.id<b.id;});
    return packets;
}
inline std::vector<Envelope> source_contracts(const WorkloadOptions&o,int shaping_budget,int guardband,double rate_factor) {
    if(shaping_budget<0||guardband<0||!std::isfinite(rate_factor)||rate_factor<=0)throw std::invalid_argument("Invalid contract parameters");
    const auto n=source_counts(o.endpoints);const auto D=critical_deadlines(o.profile);
    const long long sigma[3]={n[0]*96LL,n[1]*128LL+n[5]*1024LL,n[2]*256LL};
    const double rate[3]={n[0]*96.0/20,n[1]*128.0/50+n[5]*1024.0/2000,n[2]*256.0/100};
    std::vector<Envelope> result;
    const long long budget=static_cast<long long>(shaping_budget)+guardband;
    for(int g=0;g<3;++g){if(budget>=D[g])throw std::invalid_argument("No residual deadline after declared delay budgets");const int m=static_cast<int>(D[g]-budget);
        // Integer byte/ms rates make the operational contract exactly representable.
        result.push_back({m,static_cast<long double>(sigma[g]),std::ceil(rate[g]*rate_factor)});}
    return result;
}
struct ShaperStep {std::vector<Packet>accepted,dropped;};
class IngressShaper {
    std::array<std::deque<Packet>,3> queues_;
    std::array<long long,3> tokens_{},sigma_{},rho_{};
    std::array<int,3> last_arrival_{{-1,-1,-1}};
    int budget_,now_=-1;
public:
    IngressShaper(const std::vector<Envelope>&e,int budget):budget_(budget) {
        if(e.size()!=3||budget<0)throw std::invalid_argument("Three envelopes and nonnegative shaping budget required");
        validate_envelopes(e);
        for(int g=0;g<3;++g){if(e[g].sigma!=std::floor(e[g].sigma)||e[g].rho!=std::floor(e[g].rho)||e[g].sigma>=std::ldexp(1.L,63)||e[g].rho>=std::ldexp(1.L,63))throw std::invalid_argument("Representable integer contract required");sigma_[g]=static_cast<long long>(e[g].sigma);rho_[g]=static_cast<long long>(e[g].rho);tokens_[g]=sigma_[g];}
    }
    void add(const Packet&p){if(p.cls!=0||(p.type!=0&&p.type!=1&&p.type!=2&&p.type!=5)||p.bytes<=0||p.arrival<0||p.deadline<=p.arrival)throw std::invalid_argument("Invalid critical shaper packet");const int g=deadline_group(p);if(p.arrival<last_arrival_[g])throw std::invalid_argument("Per-class shaper arrivals must be nondecreasing");last_arrival_[g]=p.arrival;queues_[g].push_back(p);}
    ShaperStep advance(int t){if(t<0||t<=now_)throw std::invalid_argument("Shaper requires increasing nonnegative slots");ShaperStep result;
        for(int g=0;g<3;++g){if(now_>=0&&rho_[g]>0){const long long deficit=sigma_[g]-tokens_[g],elapsed=static_cast<long long>(t)-now_;
                if(elapsed>deficit/rho_[g]||(elapsed==deficit/rho_[g]&&deficit%rho_[g]==0))tokens_[g]=sigma_[g];
                else tokens_[g]+=rho_[g]*elapsed;}
            while(!queues_[g].empty()){auto p=queues_[g].front();if(p.arrival>t)break;
                if(t-p.arrival>budget_||t>=p.deadline){result.dropped.push_back(p);queues_[g].pop_front();continue;}
                if(tokens_[g]<p.bytes)break;
                tokens_[g]-=p.bytes;queues_[g].pop_front();p.queue_arrival=t;result.accepted.push_back(p);
            }}now_=t;return result;}
};
inline void export_packets(const std::string&path,const std::vector<Packet>&packets) {
    std::ofstream out(path);if(!out)throw std::runtime_error("Cannot write packet export");
    out<<"arrival_ms,class_id,size_bytes,deadline_ms,source,id,type,queue_arrival_ms\n";
    for(const auto&p:packets)out<<p.arrival<<','<<p.cls<<','<<p.bytes<<','<<p.deadline<<','<<p.source<<','<<p.id<<','<<p.type<<','<<p.queue_arrival<<'\n';
}
}
#endif
