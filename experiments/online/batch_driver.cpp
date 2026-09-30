#define main retained_simulator_main
#include "simulator.cpp"
#undef main
#include <cassert>

// Construct an intentionally tiny original-demand fixture while invoking the
// actual gate, planner, selector and atomic admission.
void configure(Simulation&s,int capacity,int demand,int deadline,int delay=0){
    s.o.capacity=capacity;s.o.delay=delay;s.o.gate_eta=1;s.o.term=1;
    s.contracts={{1,0,0}};s.B=21;s.initial=21;
    s.original_history.advance(0);
    s.outcomes.clear();s.offered.clear();
    s.outcomes.push_back({{0,1,demand,deadline,0,3,0,demand,0},demand});
    s.admit(s.outcomes[0].p,0);
}

int main(int argc,char**argv){
    if(argc!=2)return 2;
    Options options;options.context=argv[1];options.policy="luu_joint_style";
    options.duration=100;options.load=.4;options.gate_eta=-1;options.delay=2;
    options.contract_factor=1.4*options.load*options.burst_factor/.9;
    // Aggregate demand fixture: C=10, known due work=7, two 2-byte quotas.
    // q=22 is floor(10*22/106)=2; both resource reservations fit with floor21
    // and background32, so resource checking alone cannot detect this issue.
    for(bool pending:{false,true}){
        Simulation actual(options);configure(actual,10,7,1);
        TimedPlanExecutor executor(21);
        if(pending)assert(executor.propose(0,2,21,4,21,true));
        const auto old_id=executor.pending_plan_id();
        const auto old_commands=executor.counters().accepted_plan_commands;
        std::vector<Simulation::TenantProposal> batch={{22,1,10,0},{22,1,10,1}};
        assert(actual.gate_request(0,22,1));
        assert(actual.gate_luu_subset(0,batch,1));
        assert(actual.gate_luu_subset(0,batch,2));
        assert(!actual.gate_luu_subset(0,batch,3));
        assert(!actual.luu_batch_plan(0,batch,3,executor).feasible);
        const auto selected=actual.select_luu_subset(batch,[&](unsigned mask){return actual.luu_batch_plan(0,batch,mask,executor);});
        assert(selected.mask==1);
        const auto accepted=actual.admit_luu_joint_batch(0,batch,executor);
        assert(accepted.size()==1&&actual.booked(0)==22);
        if(pending){assert(executor.pending_plan_id()==old_id);assert(executor.counters().accepted_plan_commands==old_commands);}
    }
    // Partly overlapping windows: 2 bytes for one slot plus 2 for three slots.
    // A first-slot deficit is found even if the longer horizon has free space.
    {
        Simulation s(options);configure(s,10,7,1);
        std::vector<Simulation::TenantProposal>b={{22,1,10,0},{22,3,8,1}};
        assert(s.gate_luu_subset(0,b,1)&&s.gate_luu_subset(0,b,2));
        assert(!s.gate_luu_subset(0,b,3));
        // Existing booking only in slot2 must not be extended to slots0/1.
        s.outcomes[0].p.deadline=3;s.outcomes[0].remaining=18;
        s.tenant_byte_ledger[2]=3;s.ledger[2]=32;s.latest_lease_end=3;
        assert(s.gate_luu_subset(0,b,3)); // 30 - (2+6) - 3 = 19 >= 18.
        s.outcomes[0].remaining=20;
        assert(!s.gate_luu_subset(0,b,3));
    }
    // Sum floors, not floor sum: floor(10*16/106)=1 per tenant, whereas
    // floor(10*32/106)=3. Exactly eight original bytes still fit with two.
    {
        Simulation s(options);configure(s,10,8,1);
        std::vector<Simulation::TenantProposal>b={{16,1,10,0},{16,1,10,1}};
        assert(s.gate_luu_subset(0,b,3));
    }
    // A delay prefix is immutable but still participates in the demand test.
    {
        Simulation s(options);configure(s,10,27,3,2);
        std::vector<Simulation::TenantProposal>b={{22,1,10,0},{22,1,10,1}};
        assert(s.gate_luu_subset(0,b,1));assert(!s.gate_luu_subset(0,b,3));
        s.o.gate_eta=-1;assert(s.gate_luu_subset(0,b,3));
    }
    std::cout<<"PASS aggregate fixtures and actual pending/replacement admission\n";
}
