// Independent one-packet UDP pacer. No queued service and no catch-up credit.
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <time.h>
#ifdef __APPLE__
#include <mach/mach_time.h>
#endif
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

static std::uint64_t now_ns(){
#ifdef __APPLE__
    static mach_timebase_info_data_t info=[](){mach_timebase_info_data_t x;mach_timebase_info(&x);return x;}();
    return static_cast<std::uint64_t>(static_cast<__uint128_t>(mach_absolute_time())*info.numer/info.denom);
#else
    timespec t{};clock_gettime(CLOCK_MONOTONIC,&t);return static_cast<std::uint64_t>(t.tv_sec)*1000000000ULL+t.tv_nsec;
#endif
}
static void wait_until(std::uint64_t target,std::uint64_t spin_ns){
    while(true){const auto now=now_ns();if(now>=target)return;const auto left=target-now;
        if(left>spin_ns+10000){const auto sleep=left-spin_ns;timespec t{static_cast<time_t>(sleep/1000000000ULL),static_cast<long>(sleep%1000000000ULL)};nanosleep(&t,nullptr);}
        // The final bounded interval uses a busy wait. This uses ordinary user
        // CPU time, no real-time priority or privileged scheduling change.
    }
}
static std::uint64_t read_be64(const unsigned char*p){std::uint64_t n=0;for(int i=0;i<8;++i)n=(n<<8)|p[i];return n;}
int main(int argc,char**argv){
    try{
        if(argc!=6)throw std::invalid_argument("usage: pacer socket_path sink_port bytes_per_ms spin_us output.csv");
        const std::string path=argv[1];const int sink_port=std::stoi(argv[2]),capacity=std::stoi(argv[3]);
        const auto spin=static_cast<std::uint64_t>(std::stoul(argv[4]))*1000;
        if(path.size()>=sizeof(sockaddr_un{}.sun_path)||capacity<1||spin>1000000)throw std::invalid_argument("invalid pacer configuration");
        int input=socket(AF_UNIX,SOCK_DGRAM,0),output=socket(AF_INET,SOCK_DGRAM,0);
        if(input<0||output<0)throw std::runtime_error("socket creation failed");
        sockaddr_un local{};local.sun_family=AF_UNIX;std::strncpy(local.sun_path,path.c_str(),sizeof(local.sun_path)-1);
        if(bind(input,reinterpret_cast<sockaddr*>(&local),sizeof(local)))throw std::runtime_error("pacer bind failed");
        sockaddr_in sink{};sink.sin_family=AF_INET;sink.sin_port=htons(sink_port);inet_pton(AF_INET,"127.0.0.1",&sink.sin_addr);
        std::ofstream log(argv[5]);log<<"wire_id,received_ns,serialization_start_ns,finish_target_ns,egress_ns,sent_bytes,send_errno,ack_send_begin_ns,ack_send_end_ns,ack_errno\n"<<std::flush;
        std::cout<<"{\"ready\":true,\"clock_ns\":"<<now_ns()<<"}\n"<<std::flush;
        std::vector<unsigned char> payload(65536);std::uint64_t last_end=0;
        while(true){sockaddr_un peer{};socklen_t length=sizeof(peer);
            const auto n=recvfrom(input,payload.data(),payload.size(),0,reinterpret_cast<sockaddr*>(&peer),&length);
            if(n<0){if(errno==EINTR)continue;throw std::runtime_error("pacer receive failed");}
            if(n==4&&std::memcmp(payload.data(),"QUIT",4)==0)break;
            const auto received=now_ns();if(n<8)continue;
            const auto wire=read_be64(payload.data());const auto began=now_ns();
            if(began<last_end)throw std::runtime_error("pacer overlap");
            const auto duration=(static_cast<std::uint64_t>(n)*1000000ULL+capacity-1)/capacity;
            const auto target=began+duration;wait_until(target,spin);
            const auto sent=sendto(output,payload.data(),n,0,reinterpret_cast<sockaddr*>(&sink),sizeof(sink));
            const int error=sent<0?errno:0;const auto end=now_ns();last_end=end;
            const std::string ack="{\"wire_id\":"+std::to_string(wire)+",\"serialization_start_ns\":"+std::to_string(began)+
                ",\"finish_target_ns\":"+std::to_string(target)+",\"egress_ns\":"+std::to_string(end)+
                ",\"sent_bytes\":"+std::to_string(std::max<ssize_t>(0,sent))+",\"send_errno\":"+std::to_string(error)+"}";
            // Notify completion before synchronous log output. The previous
            // buffered logger could sit between egress and ACK without timestamps.
            const auto ack_begin=now_ns();
            const auto ack_sent=sendto(input,ack.data(),ack.size(),MSG_DONTWAIT,reinterpret_cast<sockaddr*>(&peer),length);
            const int ack_error=ack_sent<0?errno:0;const auto ack_end=now_ns();
            log<<wire<<','<<received<<','<<began<<','<<target<<','<<end<<','<<std::max<ssize_t>(0,sent)<<','<<error
               <<','<<ack_begin<<','<<ack_end<<','<<ack_error<<'\n'<<std::flush;
            if(ack_error)throw std::runtime_error("pacer acknowledgment failed");
        }
        log.flush();close(input);close(output);unlink(path.c_str());return 0;
    }catch(const std::exception&e){std::cerr<<e.what()<<'\n';return 1;}
}
