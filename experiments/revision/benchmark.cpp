#include "../online/request_admission.hpp"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <sys/resource.h>
using namespace guarded_release;
int main(int argc,char**argv){try{
 int J=100,K=3,H=512,U=54,B=100,repeats=30,seed=1;std::string output;
 for(int i=1;i<argc;++i){std::string k=argv[i];if(i+1==argc)throw std::runtime_error("Missing value");std::string v=argv[++i];
 if(k=="--jobs")J=std::stoi(v);else if(k=="--classes")K=std::stoi(v);else if(k=="--horizon")H=std::stoi(v);else if(k=="--floors")U=std::stoi(v);else if(k=="--bookings")B=std::stoi(v);else if(k=="--repeats")repeats=std::stoi(v);else if(k=="--seed")seed=std::stoi(v);else if(k=="--output")output=v;else throw std::runtime_error("Unknown option");}
 if(output.empty()||J<0||K<1||K>100||H<2*K+25||H>100000||U<1||U>400||B<0||repeats<1)throw std::runtime_error("Invalid benchmark");
 auto construct=std::chrono::steady_clock::now();Snapshot s;s.capacity=100000;s.units=512;s.base=16;s.maximum=s.base+U-1;s.step=5;s.delay=2;s.term=10;
 int rate=static_cast<int>(floor_service(s,s.maximum));for(int k=0;k<K;++k)s.envelopes.push_back({20+2*k,1,static_cast<long double>(rate/K+(k<rate%K))});
 std::mt19937 rng(seed);for(int j=0;j<J;++j)s.pending.push_back({H-1,static_cast<long double>(1+rng()%3)});
 s.old_units.assign(H,s.maximum);s.other_units.assign(H,0);
 for(int b=0;b<B;++b)++s.other_units[rng()%H];
 for(int x:s.other_units)if(x+s.maximum>s.units)throw std::runtime_error("Infeasible booking fixture");
 double construction=std::chrono::duration<double,std::micro>(std::chrono::steady_clock::now()-construct).count();
 std::vector<double> times;std::size_t calls=0;
 for(int i=0;i<repeats+5;++i){auto start=std::chrono::steady_clock::now();auto d=guarded_decision(s);double us=std::chrono::duration<double,std::micro>(std::chrono::steady_clock::now()-start).count();
 if(!d.certified)throw std::runtime_error("Uncertified fixture");if(i>=5){times.push_back(us);calls+=d.feasibility_calls;}}
 std::sort(times.begin(),times.end());struct rusage usage{};getrusage(RUSAGE_SELF,&usage);
 std::ofstream out(output);if(!out)throw std::runtime_error("Cannot write output");
 out<<std::setprecision(17)<<"{\"status\":\"COMPLETE\",\"jobs\":"<<J<<",\"classes\":"<<K<<",\"horizon\":"<<H<<",\"floors\":"<<U<<",\"bookings\":"<<B<<",\"seed\":"<<seed<<",\"repeats\":"<<repeats<<",\"construction_us\":"<<construction<<",\"p50_us\":"<<times[times.size()/2]<<",\"p95_us\":"<<times[static_cast<std::size_t>(std::ceil(.95*times.size()))-1]<<",\"max_us\":"<<times.back()<<",\"max_rss_kb\":"<<usage.ru_maxrss<<",\"certificate_calls\":"<<calls<<",\"scope\":\"Linux process memory; construction separate; repeated timings are not independent workload samples\"}\n";
 }catch(const std::exception&e){std::cerr<<e.what()<<'\n';return 2;}return 0;}
